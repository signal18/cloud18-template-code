// Cloud18 sample .NET application: says hello and proves the database path end to end.
// The template passes the connection through the environment (DB_HOST is the cluster's
// proxy, so the answer comes from the current primary and follows a switchover).
using System.Net;
using MySqlConnector;

static string Env(string key, string def)
{
    var v = Environment.GetEnvironmentVariable(key);
    return string.IsNullOrEmpty(v) ? def : v;
}

var host = Env("DB_HOST", "127.0.0.1");
var port = Env("DB_PORT", "3306");
var name = Env("DB_NAME", "");
var connection = new MySqlConnectionStringBuilder
{
    Server = host,
    Port = uint.Parse(port),
    Database = name,
    UserID = Env("DB_USER", ""),
    Password = Env("DB_PASSWORD", ""),
    ConnectionTimeout = 3,
}.ConnectionString;

var app = WebApplication.CreateBuilder(args).Build();

app.MapGet("/", async () =>
{
    var status = 200;
    string title, detail;
    try
    {
        await using var db = new MySqlConnection(connection);
        await db.OpenAsync();
        await using var cmd = new MySqlCommand("SELECT @@server_id, @@hostname, VERSION()", db);
        await using var row = await cmd.ExecuteReaderAsync();
        await row.ReadAsync();
        title = $"Hello world, I'm connected to database server id {row.GetValue(0)}";
        detail = $".NET, {row.GetString(1)}, {row.GetString(2)}, schema {name}, through {host}:{port}";
    }
    catch (Exception err)
    {
        status = 503;
        title = "Hello world, I'm not connected to the database";
        detail = err.Message;
    }
    var body = "<!DOCTYPE html>\n<html>\n  <head>\n    <meta charset=\"utf-8\">\n    <title>Cloud18 sample .NET application</title>\n  </head>\n  <body>\n    <h1>"
        + WebUtility.HtmlEncode(title) + "</h1>\n    <p>" + WebUtility.HtmlEncode(detail) + "</p>\n  </body>\n</html>\n";
    return Results.Content(body, "text/html; charset=utf-8", statusCode: status);
});

app.Run("http://0.0.0.0:" + Env("PORT", "8080"));
