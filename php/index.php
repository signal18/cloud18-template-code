<?php
// Cloud18 sample PHP application: says hello and proves the database path end to end.
// The template passes the connection through the environment (DB_HOST is the cluster's
// proxy, so the answer comes from the current primary and follows a switchover).
$host = getenv('DB_HOST') ?: '127.0.0.1';
$port = (int) (getenv('DB_PORT') ?: 3306);
$name = getenv('DB_NAME') ?: '';
$user = getenv('DB_USER') ?: '';
$pass = getenv('DB_PASSWORD') ?: '';

$line = '';
$error = '';
mysqli_report(MYSQLI_REPORT_OFF);
$db = mysqli_init();
$db->options(MYSQLI_OPT_CONNECT_TIMEOUT, 3);
if (@$db->real_connect($host, $user, $pass, $name, $port)) {
    $row = $db->query('SELECT @@server_id AS id, @@hostname AS host, VERSION() AS version')->fetch_assoc();
    $line = sprintf("Hello world, I'm connected to database server id %d", $row['id']);
    $detail = sprintf('%s, %s, schema %s, through %s:%d', $row['host'], $row['version'], $name, $host, $port);
    $db->close();
} else {
    http_response_code(503);
    $error = mysqli_connect_error();
}
?>
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Cloud18 sample PHP application</title>
  </head>
  <body>
<?php if ($line !== '') { ?>
    <h1><?= htmlspecialchars($line) ?></h1>
    <p><?= htmlspecialchars($detail) ?></p>
<?php } else { ?>
    <h1>Hello world, I'm not connected to the database</h1>
    <p><?= htmlspecialchars($error) ?></p>
<?php } ?>
  </body>
</html>
