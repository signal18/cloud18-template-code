// Cloud18 sample Next.js application: says hello and proves the database path end to end.
// The template passes the connection through the environment (DB_HOST is the cluster's
// proxy, so the answer comes from the current primary and follows a switchover).
import mysql from 'mysql2/promise'

// rendered on the server at every request: the page must show the live server id
export const dynamic = 'force-dynamic'

export default async function Page() {
  const host = process.env.DB_HOST || '127.0.0.1'
  const port = Number(process.env.DB_PORT || 3306)
  const name = process.env.DB_NAME || ''
  let title = ''
  let detail = ''
  try {
    const db = await mysql.createConnection({
      host,
      port,
      user: process.env.DB_USER || '',
      password: process.env.DB_PASSWORD || '',
      database: name,
      connectTimeout: 3000,
    })
    const [rows] = await db.query('SELECT @@server_id AS id, @@hostname AS host, VERSION() AS version')
    await db.end()
    title = `Hello world, I'm connected to database server id ${rows[0].id}`
    detail = `Next.js, ${rows[0].host}, ${rows[0].version}, schema ${name}, through ${host}:${port}`
  } catch (err) {
    title = "Hello world, I'm not connected to the database"
    detail = String(err.message || err)
  }
  return (
    <main>
      <h1>{title}</h1>
      <p>{detail}</p>
    </main>
  )
}
