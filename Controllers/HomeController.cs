using log4net;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using ClosedXML.Excel;
using System.Text.RegularExpressions;

namespace Hangfire.Controllers
{
    public class HomeController : Controller
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(HomeController));
        private readonly IBackgroundJobClient _jobClient;
        private readonly IConfiguration _configuration;

        public HomeController(IBackgroundJobClient jobClient, IConfiguration configuration)
        {
            _jobClient = jobClient;
            _configuration = configuration;
        }
        public IActionResult Index()
        {
            try {
                PopulateGeneratedReports();
                var monitor = JobStorage.Current.GetMonitoringApi();
                var enqueuedJobs = monitor.EnqueuedJobs("default", 0, 10);

                foreach (var job in enqueuedJobs)
                {
                    log.Info($"On Load JobId: {job.Key} : {job.Value}");
                }
                

            }
            catch (Exception ex) {
                log.Info("No Items in queue");
            }

            return View();
        }

        private void PopulateGeneratedReports()
        {
            //TODO: WRITE CODE TO JUST FETCH THE NAMES OF THE REPORTS THAT ARE CREATED SO THAT THE USER CAN DOWNLOAD THEM
        }

        [HttpPost]
        public async Task<IActionResult> Upload(IFormFile file)
        {
            try
            {
                log.Info("=== [UPLOAD] datasubmit_Click triggered ===");
                LogHangfireQueueState("PRE-UPLOAD");
               

                if (file != null && file.Length > 0)
                {
                    string oldName = file.FileName;
                    string newName = "FILE_" + Guid.NewGuid().ToString() 
                        + Path.GetExtension(file.FileName); var uploadPath = 
                        Path.Combine(Directory.GetCurrentDirectory(), "wwwroot/Uploads");

                    if (!Directory.Exists(uploadPath))
                    {
                        Directory.CreateDirectory(uploadPath);
                    }

                    log.Info($"[UPLOAD] Original file: '{oldName}' | Saved As to: '{newName}'");

                    var path = Path.Combine(uploadPath, newName);

                    using (var stream = new FileStream(path, FileMode.Create))
                    {
                        await file.CopyToAsync(stream);
                    }

                    log.Info($"File uploaded successfully: {newName}");


                    // STEP 1: Parse Excel → DataTable
                    log.Info("[UPLOAD] STEP 1 — Reading Excel file into DataTable...");
                    DataTable dt = ReadExcelToDataTable(
                        file,
                        oldName,
                        newName
                    );

                    // STEP 2: Bulk insert into Oracle staging table
                    log.Info("[UPLOAD] STEP 2 — Starting Oracle bulk insert...");
                    BulkInsertToOracle(dt);
                    log.Info("[UPLOAD] STEP 2 DONE — Bulk insert completed successfully.");

                    // STEP 3: Add on queue
                    var jobId = _jobClient.Enqueue(() => RunBatch(newName));
                    LogHangfireQueueState("POST-UPLOAD");
                    log.Info($"[UPLOAD] === COMPLETE — File '{newName}' uploaded and job '{jobId}' queued. ===");

                    // STEP 4: Persist file metadata via stored procedure
                    log.Info("[UPLOAD] STEP 3 — Saving file metadata to DB...");
                    SaveFileMetadata(oldName, newName, jobId);
                    log.Info("[UPLOAD] STEP 3 DONE — Metadata saved.");

                }
                else
                {
                    log.Warn("No file uploaded");
                }
            }
            catch (Exception ex)
            {
                log.Error($"Error: {ex.ToString()}");
            }

            return RedirectToAction("Index");
        }

       

        // ----------------------------------------------------------------
        // HANGFIRE JOB: Receives fileName from the queue (not from DB).
        // The rest of batch processing is delegated to BatchProcessor.
        // ----------------------------------------------------------------
        [JobDisplayName("Batch Process: {0}")]
        [AutomaticRetry(Attempts = 3)]
        public async Task RunBatch(string fileName)
        {
            log.Info($"[JOB] === Hangfire job started for file: '{fileName}' ===");

            // Log queue state at job start as proof of execution context
            LogHangfireQueueState($"JOB-START [{fileName}]");

            try
            {
                // Intentional delay — allows upstream DB writes to fully commit
                // before the batch processor begins reading records.
                log.Info($"[JOB] Waiting 5 seconds for DB commit propagation — File: '{fileName}'");
                await Task.Delay(TimeSpan.FromSeconds(5));
                log.Info($"[JOB] Wait complete. Proceeding to batch processing — File: '{fileName}'");

                // Delegate the actual batch work to BatchProcessor,
                // passing the fileName received from the Hangfire queue.
                var processor = new BatchProcessor(_configuration);
                await processor.ProcessFile(fileName);

                log.Info($"[JOB] === Hangfire job COMPLETED successfully for file: '{fileName}' ===");
            }
            catch (Exception ex)
            {
                log.Error($"[JOB] === Hangfire job FAILED for file: '{fileName}' ===", ex);
                throw; // Re-throw so Hangfire marks the job as failed and retries per AutomaticRetry policy
            }
        }


        // ----------------------------------------------------------------
        // HELPER: Logs current Hangfire queue snapshot for audit trail
        // ----------------------------------------------------------------
        private void LogHangfireQueueState(string context)
        {
            try
            {
                var monitor = JobStorage.Current.GetMonitoringApi();
                var queues = monitor.Queues();
                var enqueued = monitor.EnqueuedJobs("default", 0, 10);

                log.Info($"[HANGFIRE-MONITOR | {context}] Queue count: {queues.Count}");

                foreach (var q in queues)
                    log.Info($"[HANGFIRE-MONITOR | {context}] Queue='{q.Name}' | Length={q.Length}");

                log.Info($"[HANGFIRE-MONITOR | {context}] Enqueued jobs (default, top 10): {enqueued.Count}");

                foreach (var job in enqueued)
                    log.Info($"[HANGFIRE-MONITOR | {context}] JobId='{job.Key}' | State='{job.Value?.State}' | Created='{job.Value?.EnqueuedAt}'");
            }
            catch (Exception ex)
            {
                log.Warn($"[HANGFIRE-MONITOR] Could not retrieve queue state for context '{context}'.", ex);
            }
        }

        // ----------------------------------------------------------------
        // BULK INSERT: Writes parsed Excel rows into Oracle staging table
        // ----------------------------------------------------------------
        private void BulkInsertToOracle(DataTable dt)
        {
            log.Info("[BULK-INSERT] Opening Oracle connection...");

            var connectionString = _configuration.GetConnectionString("OracleDb");

            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                log.Info("[BULK-INSERT] Oracle connection opened.");

                using (var bulkCopy = new OracleBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "T_EXCEL_BULK_UPLOAD";

                    bulkCopy.ColumnMappings.Add("PROPOSALNO", "PROPOSALNO");
                    bulkCopy.ColumnMappings.Add("NAME", "NAME");
                    bulkCopy.ColumnMappings.Add("DOB", "DOB");
                    bulkCopy.ColumnMappings.Add("AADHAARNO", "AADHAARNO");
                    bulkCopy.ColumnMappings.Add("PAN", "PAN");
                    bulkCopy.ColumnMappings.Add("MOBILE", "MOBILE");
                    bulkCopy.ColumnMappings.Add("ADDRESS", "ADDRESS");
                    bulkCopy.ColumnMappings.Add("EMAIL", "EMAIL");
                    bulkCopy.ColumnMappings.Add("VOTERID", "VOTERID");
                    bulkCopy.ColumnMappings.Add("PASSPORTNUMBER", "PASSPORTNUMBER");
                    bulkCopy.ColumnMappings.Add("DRIVINGLICENSE", "DRIVINGLICENSE");
                    bulkCopy.ColumnMappings.Add("BANKACCOUNT", "BANKACCOUNT");
                    bulkCopy.ColumnMappings.Add("CIN", "CIN");
                    bulkCopy.ColumnMappings.Add("UIN", "UIN");
                    bulkCopy.ColumnMappings.Add("GSTIN", "GSTIN");
                    bulkCopy.ColumnMappings.Add("IS_PROCESSED", 0);
                    bulkCopy.ColumnMappings.Add("ORIGINAL_FILE_NAME", "ORIGINAL_FILE_NAME");
                    bulkCopy.ColumnMappings.Add("NEW_FILE_NAME", "NEW_FILE_NAME");

                    log.Info($"[BULK-INSERT] Inserting {dt.Rows.Count} rows into '{bulkCopy.DestinationTableName}'...");
                    bulkCopy.WriteToServer(dt);
                    log.Info("[BULK-INSERT] WriteToServer completed — all rows inserted.");
                }
            }

            log.Info("[BULK-INSERT] Oracle connection closed.");
        }

        // ----------------------------------------------------------------
        // METADATA: Stores file record in DB via stored procedure
        // ----------------------------------------------------------------
        private void SaveFileMetadata(string originalFileName, string newFileName, string jobId)
        {
           
            // Resolve logged-in username from session/context
            string currentUser = "GET_YOUR_CURRENT_USER";
            log.Info($"[METADATA] Username resolved from session: '{currentUser}'");

            var connectionString = _configuration.GetConnectionString("OracleDb");

            using (var conn = new OracleConnection(connectionString))
            using (var cmd = new OracleCommand("PROC_FILE_METADATA", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add("ORIGINAL_FILE_NAME", OracleDbType.Varchar2).Value = originalFileName;
                cmd.Parameters.Add("NEW_FILE_NAME", OracleDbType.Varchar2).Value = newFileName;
                cmd.Parameters.Add("USERNAME", OracleDbType.Varchar2).Value = currentUser;
                cmd.Parameters.Add("JobID", OracleDbType.Varchar2).Value = jobId;

                conn.Open();

                cmd.ExecuteNonQuery();
            }

            log.Info($"[METADATA] Stored procedure PROC_FILE_METADATA executed successfully.");
        }


        public DataTable ReadExcelToDataTable(IFormFile file, string originalFileName, string newFileName)
        {
            DataTable dt = new DataTable();
            using (var fileStream = file.OpenReadStream())
            using (var workbook = new XLWorkbook(fileStream))
            {

                var worksheet = workbook.Worksheets.FirstOrDefault();

                if (worksheet == null)
                    return dt;

                bool firstRow = true;

                foreach (var row in worksheet.RowsUsed())
                {
                    if (firstRow)
                    {
                        // 🔹 HEADER PROCESSING
                        foreach (var cell in row.Cells())
                        {
                            string header = cell.Value.ToString();
                            string normalized = Normalize(header.ToLower());

                            if (normalized.Contains("proposal"))
                                header = "PROPOSALNO";
                            else if (normalized.Contains("aadhaar") || normalized.Contains("aadhar"))
                                header = "AADHAARNO";
                            else if (normalized.Contains("voter"))
                                header = "VOTERID";
                            else if (normalized.Contains("passport"))
                                header = "PASSPORTNUMBER";
                            else if (normalized.Contains("driving"))
                                header = "DRIVINGLICENSE";
                            else if (normalized.Contains("bank"))
                                header = "BANKACCOUNT";
                            else if (normalized.Contains("cin"))
                                header = "CIN";
                            else if (normalized.Contains("uin"))
                                header = "UIN";

                            dt.Columns.Add(header.Trim().ToUpper(), typeof(string));
                        }

                        // 🔹 METADATA COLUMNS
                        dt.Columns.Add("IS_PROCESSED", typeof(int)).DefaultValue = 0;
                        dt.Columns.Add("ORIGINAL_FILE_NAME", typeof(string)).DefaultValue = originalFileName;
                        dt.Columns.Add("NEW_FILE_NAME", typeof(string)).DefaultValue = newFileName;

                        firstRow = false;
                    }
                    else
                    {
                        // 🔹 DATA ROW PROCESSING
                        DataRow newRow = dt.NewRow();
                        int columnIndex = 0;

                        foreach (var cell in row.Cells(1, dt.Columns.Count - 3))
                        {
                            object value = cell.Value;

                            if (value is DateTime dtValue)
                            {
                                newRow[columnIndex] = dtValue.ToString("dd/MM/yyyy");
                            }
                            else
                            {
                                newRow[columnIndex] = value?.ToString() ?? string.Empty;
                            }

                            columnIndex++;
                        }

                        dt.Rows.Add(newRow);
                    }
            }
            }

            dt.AcceptChanges();
            return dt;
        }

        private string Normalize(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return string.Empty;

            // Remove special characters, dots, spaces, etc.
            input = Regex.Replace(input, @"[^a-z0-9]", "");

            return input;
        }
    }
}
