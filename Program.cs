using log4net.Config;
using log4net;
using System.Reflection;
using Hangfire;
using Hangfire.LiteDB;
using Microsoft.AspNetCore.RateLimiting;

var builder = WebApplication.CreateBuilder(args);



var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));

builder.Logging.AddLog4Net("log4net.config");
builder.Services.AddHangfireServer(options =>
{
    options.WorkerCount = 4;
});

builder.Services.AddHangfire(config =>
    config.UseLiteDbStorage("hangfire.db")
);

builder.Services.AddRateLimiter(
    options =>
    {
        options.AddFixedWindowLimiter(policyName: "fixed", options =>
        {
            options.PermitLimit = 10; // 10 uploads per window
            options.Window = TimeSpan.FromSeconds(60);
        });
    }
    );

// Add services to the container.
builder.Services.AddControllersWithViews();

var app = builder.Build();

app.UseHangfireDashboard();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}
app.UseHttpsRedirection();
app.UseHsts();

app.UseRouting();

app.UseAuthorization();

app.MapStaticAssets();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}")
    .WithStaticAssets();


app.Run();
