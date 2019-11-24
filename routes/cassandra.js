const express = require("express");
const router = express.Router();
const uuidv4 = require("uuid/v4");
const cassandra = require("cassandra-driver");
const client = new cassandra.Client({
  contactPoints: ["localhost"],
  localDataCenter: "datacenter1",
  keyspace: "socnet"
});

// Init

client
  .execute(
    `CREATE KEYSPACE IF NOT EXISTS socnet WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`
  )
  .then(
    () =>
      client.execute(
        `CREATE TABLE IF NOT EXISTS users_by_id(UserID uuid, Name text, Email text, PRIMARY KEY(UserID, Email));`,
        (err, result) => {
          console.log(err, result);
        }
      ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS users_by_email(UserID uuid, Name text, Email text, PRIMARY KEY(Email, Name));`,
      (err, result) => {
        console.log(err, result);
      }
    ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS posts(PostID uuid, UserID uuid, Content text, PRIMARY KEY(PostID, UserID));`,
      (err, result) => {
        console.log(err, result);
      }
    ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS messages(MsgID uuid, UserID uuid, RecipientID uuid, Text text, PRIMARY KEY(MsgID, UserID));`,
      (err, result) => {
        console.log(err, result);
      }
    )
  );
// Routes
router.post("/user", async (req, res) => {
  const { name, email } = req.body;
  const uid = uuidv4(email);
  try {
    const query1 =
      "INSERT INTO users_by_id (UserID, Name, Email) VALUES (?,?,?) IF NOT EXISTS;";
    const query2 =
      "INSERT INTO users_by_email (UserID, Name, Email) VALUES (?,?,?) IF NOT EXISTS;";
    await client
      .execute(query1, [uid, name, email], { prepare: true })
      .then(result => {
        console.log(result);
      });
    await client
      .execute(query2, [uid, name, email], { prepare: true })
      .then(result => {
        res.status(200).json({ message: "Successfully create a new user!" });
        console.log(result);
      });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.get("/user/id/:id", async (req, res) => {
  const id = req.params.id;
  try {
    const query = "SELECT * FROM users_by_id WHERE UserID = ?;";
    client.execute(query, [id], { prepare: true }).then(result => {
      const row = result.first();
      res.status(200).json(row);
      console.log(row);
    });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.get("/user/email/:email", async (req, res) => {
  const email = req.params.email;
  try {
    const query = "SELECT * FROM users_by_email WHERE Email = ?;";
    client.execute(query, [email], { prepare: true }).then(result => {
      const row = result.first();
      res.status(200).json(row);
      console.log(row);
    });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

module.exports = router;
