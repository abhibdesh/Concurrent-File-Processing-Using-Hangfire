// ============================================================
// BatchProcessor.cs
// Processes a single file passed from the Hangfire queue.
// Searches records in parallel and exports results to Excel.
// ============================================================

using System.Data;
using System.Diagnostics;
using System.Reflection;
using ClosedXML.Excel;
using log4net;
using Oracle.ManagedDataAccess.Client;

public class BatchProcessor(IConfiguration _configuration)
{
    private static readonly ILog log = LogManager.GetLogger(typeof(BatchProcessor));

    // ----------------------------------------------------------------
    // ENTRY POINT: Called by Hangfire via UploadHandler.RunBatch().
    // Receives fileName from the queue — no DB fetch needed for the name.
    // ----------------------------------------------------------------
    public async Task ProcessFile(string fileName)
    {
        log.Info($"[BATCH] === RunBatch started for file: '{fileName}' ===");
        Stopwatch mainStopwatch = Stopwatch.StartNew();

        try
        {
            // Fetch the file's DB record using the fileName as the key.
            // This retrieves FILE_ID and confirms the file is registered.
            log.Info($"[BATCH] Fetching DB record for file: '{fileName}'...");
         
            // Fetch all records from the staging table for this file
            log.Info($"[BATCH] Fetching staging records for file: '{fileName}'...");
            DataTable records = FetchRecordsToBeSearched(fileName);
            long fileId = GetFileId(fileName);

            if (records == null || records.Rows.Count == 0)
            {
                log.Warn($"[BATCH] No staging records found for file: '{fileName}'. Nothing to process.");
                return;
            }

            log.Info($"[BATCH] {records.Rows.Count} staging records fetched for file: '{fileName}'.");

            // Run parallel AML search across all records
            DataTable finalResults = await RunParallelSearch(records, fileName);

            if (finalResults.Rows.Count > 0)
            {
                log.Info($"[BATCH] {finalResults.Rows.Count} match(es) found. Generating Excel report...");

                string reportName = $"Report_{DateTime.Now:ddMMyyyyHHmmss}.xlsx";
                Stopwatch excelTimer = Stopwatch.StartNew();

                ExportToExcel(finalResults, reportName, fileId);

                log.Info($"[BATCH] Excel report '{reportName}' generated in {excelTimer.Elapsed.TotalSeconds:F2}s.");
            }
            else
            {
                log.Warn($"[BATCH] No matches found for file: '{fileName}'. No report generated.");
            }

            log.Info($"[BATCH] === RunBatch COMPLETED for file: '{fileName}' in {mainStopwatch.Elapsed.TotalMinutes:F2} minutes ===");
        }
        catch (Exception ex)
        {
            log.Error($"[BATCH] === RunBatch FAILED for file: '{fileName}' after {mainStopwatch.Elapsed.TotalSeconds:F1}s ===", ex);
            throw; // Propagate to Hangfire for retry tracking
        }
    }

    // ----------------------------------------------------------------
    // PARALLEL SEARCH: Searches each record concurrently via SP.
    // Thread-safe merge into a single result DataTable.
    // ----------------------------------------------------------------
    private async Task<DataTable> RunParallelSearch(DataTable records, string fileName)
    {
        log.Info($"[PARALLEL] Starting parallel search — {records.Rows.Count} records | MaxDegreeOfParallelism: 4 | File: '{fileName}'");
        Stopwatch parallelTimer = Stopwatch.StartNew();

        // Final results table — schema built from first SP result
        DataTable finalResults = new DataTable();
        bool schemaInitialized = false;
        object schemaLock = new object();

        int matched = 0;
        int searched = 0;

        await Task.Run(() =>
        {
            Parallel.ForEach(
                records.AsEnumerable(),
                new ParallelOptions { MaxDegreeOfParallelism = 4 },
                record =>
                {
                    string searchValue = BuildSearchValue(record);

                    if (string.IsNullOrWhiteSpace(searchValue))
                    {
                        log.Warn($"[PARALLEL] Skipping record — all fields empty.");
                        return;
                    }

                    Interlocked.Increment(ref searched);
                    log.Info($"[SP] Calling search SP for value: '{searchValue}'");

                    DataTable spResult = CallSearchProcedure(searchValue);

                    if (spResult == null || spResult.Rows.Count == 0)
                    {
                        log.Info($"[SP] No match returned for: '{searchValue}'");
                        return;
                    }

                    log.Info($"[SP] {spResult.Rows.Count} match(es) found for: '{searchValue}'");
                   Interlocked.Add(ref matched, spResult.Rows.Count);

                    lock (schemaLock)
                    {
                        // Initialize schema once from the first result table
                        if (!schemaInitialized)
                        {
                            foreach (DataColumn col in spResult.Columns)
                                finalResults.Columns.Add(col.ColumnName, col.DataType);

                            schemaInitialized = true;
                            log.Info($"[PARALLEL] Result schema initialized with {finalResults.Columns.Count} column(s).");
                        }

                        foreach (DataRow r in spResult.Rows)
                        {
                            DataRow newRow = finalResults.NewRow();
                            foreach (DataColumn col in spResult.Columns)
                                newRow[col.ColumnName] = r[col];
                            finalResults.Rows.Add(newRow);
                        }
                    }
                });
        });

        log.Info($"[PARALLEL] Search complete — Records searched: {searched} | Total matches: {matched} | Time: {parallelTimer.Elapsed.TotalSeconds:F2}s | File: '{fileName}'");

        return finalResults;
    }

    // ----------------------------------------------------------------
    // BUILD SEARCH STRING: Concatenates non-null field values
    // ----------------------------------------------------------------
    private string BuildSearchValue(DataRow record)
    {
        var values = record.ItemArray
            .Where(x => x != DBNull.Value && !string.IsNullOrWhiteSpace(x?.ToString()))
            .Select(x => x.ToString());

        return string.Join(", ", values);
    }

    // ----------------------------------------------------------------
    // STORED PROCEDURE CALL: Executes the search SP for a given value
    // ----------------------------------------------------------------
    private DataTable CallSearchProcedure(string searchValue)
    {
        DataTable result = new DataTable();

        try
        {
            string procName = "SCHEMA_NAME.SP_SEARCH_DETAILS"; 

            var connectionString = _configuration.GetConnectionString("OracleDb");

            using (var conn = new OracleConnection(connectionString))
            using (var cmd = new OracleCommand("PROC_FETCH_RECORDS", conn)) // your actual SP name
            {
                cmd.CommandType = CommandType.StoredProcedure;

                // 🔹 Input parameter
                cmd.Parameters.Add("SEACRH_VALUE", OracleDbType.Varchar2).Value = searchValue;

                // 🔹 Output cursor (IMPORTANT)
                cmd.Parameters.Add("P_OUTPUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                conn.Open();

                using (var adapter = new OracleDataAdapter(cmd))
                {
                    adapter.Fill(result);
                }
            }
        }
        catch (Exception ex)
        {
            string context = $"Method: {MethodBase.GetCurrentMethod()?.Name} | Input: '{searchValue}'";
            log.Error($"[SP] Exception during stored procedure call — {context}", ex);
        }

        return result;
    }

    // ----------------------------------------------------------------
    // EXCEL EXPORT: Writes result DataTable to .xlsx and updates DB
    // ----------------------------------------------------------------
    public void ExportToExcel(DataTable dt, string reportName, long fileId)
    {
        log.Info($"[EXCEL] Starting export — Report: '{reportName}' | Rows: {dt.Rows.Count} | FileId: {fileId}");

        try
        {
            string folderPath = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot", "Reports");

            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
                log.Info($"[EXCEL] Reports directory created at: '{folderPath}'");
            }

            string filePath = Path.Combine(folderPath, reportName);

            using (XLWorkbook wb = new XLWorkbook())
            {
                wb.Worksheets.Add(dt, "Report");
                wb.SaveAs(filePath);
            }

            log.Info($"[EXCEL] Workbook saved to: '{filePath}'");

            // Update DB to mark this file as processed
            log.Info($"[EXCEL] Updating DB — marking FileId: {fileId} as processed with report: '{reportName}'");
            Stopwatch dbTimer = Stopwatch.StartNew();

            var connectionString = _configuration.GetConnectionString("OracleDb");

            using (var conn = new OracleConnection(connectionString))
            using (var cmd = new OracleCommand("UPDATE_FILE_DATA", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add("REPORT_NAME", OracleDbType.Varchar2).Value = reportName;
                cmd.Parameters.Add("FILE_ID", OracleDbType.Varchar2).Value = fileId;

                conn.Open();

                cmd.ExecuteNonQuery();
            }


            log.Info($"[EXCEL] DB updated in {dbTimer.Elapsed.TotalSeconds:F2}s — FileId: {fileId} | Report: '{reportName}'");
            log.Info($"[EXCEL] === Export complete for report: '{reportName}' ===");
        }
        catch (Exception ex)
        {
            log.Error($"[EXCEL] Export FAILED for report: '{reportName}' | FileId: {fileId}", ex);
            throw;
        }
    }

    public DataTable FetchRecordsToBeSearched(string fileName)
    {
        DataTable result = new DataTable();

        var connectionString = _configuration.GetConnectionString("OracleDb");

        using (var conn = new OracleConnection(connectionString))
        using (var cmd = new OracleCommand("PROC_FETCH_RECORDS", conn)) // your actual SP name
        {
            cmd.CommandType = CommandType.StoredProcedure;

            // 🔹 Input parameter
            cmd.Parameters.Add("P_FILE_NAME", OracleDbType.Varchar2).Value = fileName;

            // 🔹 Output cursor (IMPORTANT)
            cmd.Parameters.Add("P_OUTPUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

            conn.Open();

            using (var adapter = new OracleDataAdapter(cmd))
            {
                adapter.Fill(result);
            }
        }

        return result;
    }

    public long GetFileId(string fileName)
    {
        // FETCH YOUR FILE ID
        return 0;
    }
}