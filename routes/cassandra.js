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
        `CREATE TABLE IF NOT EXISTS users_by_id(UserID uuid, Name text, Email text, PRIMARY KEY(UserID));`,
        (err, result) => {
          console.log(err, result);
        }
      ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS users_by_email(UserID uuid, Name text, Email text, PRIMARY KEY(Email));`,
      (err, result) => {
        console.log(err, result);
      }
    ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS posts_by_id(PostID uuid, UserID uuid, Content text, PRIMARY KEY(PostID, UserID));`,
      (err, result) => {
        console.log(err, result);
      }
    ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS posts_by_user(PostID uuid, UserID uuid, Content text, PRIMARY KEY(UserID, PostID));`,
      (err, result) => {
        console.log(err, result);
      }
    ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS messages_by_id(MsgID uuid, UserID uuid, RecipientID uuid, Text text, PRIMARY KEY(MsgID, UserID));`,
      (err, result) => {
        console.log(err, result);
      }
    ),
    client.execute(
      `CREATE TABLE IF NOT EXISTS messages_by_user(MsgID uuid, UserID uuid, RecipientID uuid, Text text, PRIMARY KEY(UserID, MsgID));`,
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

router.post("/post", async (req, res) => {
  const { userid, content } = req.body;
  const postid = uuidv4(content);
  try {
    const query1 =
      "INSERT INTO posts_by_id (PostID, UserID, Content) VALUES (?,?,?) IF NOT EXISTS;";
    const query2 =
      "INSERT INTO posts_by_user (PostID, UserID, Content) VALUES (?,?,?) IF NOT EXISTS;";
    await client
      .execute(query1, [postid, userid, content], { prepare: true })
      .then(result => {
        console.log(result);
      });
    await client
      .execute(query2, [postid, userid, content], { prepare: true })
      .then(result => {
        res.status(200).json({ message: "Successfully created a new post!" });
        console.log(result);
      });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.post("/message", async (req, res) => {
  const { userid, recipientid, text } = req.body;
  const msgid = uuidv4(recipientid+text);
  try {
    const query1 =
      "INSERT INTO messages_by_id (MsgID, UserID, RecipientID, Text) VALUES (?,?,?,?) IF NOT EXISTS;";
    const query2 =
      "INSERT INTO messages_by_user (MsgID, UserID, RecipientID, Text) VALUES (?,?,?,?) IF NOT EXISTS;";
    await client
      .execute(query1, [msgid, userid, recipientid, text], { prepare: true })
      .then(result => {
        console.log(result);
      });
    await client
      .execute(query2, [msgid, userid, recipientid, text], { prepare: true })
      .then(result => {
        res.status(200).json({ message: "Successfully created a new message!" });
        console.log(result);
      });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.get("/post/:id", async (req, res) => {
  const pid = req.params.id;
  try {
    const query = "SELECT * FROM posts_by_id WHERE PostID = ?;";
    client.execute(query, [pid], { prepare: true }).then(result => {
      const row = result.first();
      res.status(200).json(row);
      console.log(row);
    });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.get("/message/:id", async (req, res) => {
  const pid = req.params.id;
  try {
    const query = "SELECT * FROM messages_by_id WHERE MsgID = ?;";
    client.execute(query, [pid], { prepare: true }).then(result => {
      const row = result.first();
      res.status(200).json(row);
      console.log(row);
    });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.get("/user/:id/messages", async (req, res) => {
  const uid = req.params.id;
  try {
    const query = "SELECT * FROM messages_by_user WHERE UserID = ?;";
    client.execute(query, [uid], { prepare: true }).then(result => {
      res.status(200).json(result.rows);
    });
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

router.get("/user/:id/posts", async (req, res) => {
  const uid = req.params.id;
  try {
    const query = "SELECT * FROM posts_by_user WHERE UserID = ?;";
    client.execute(query, [uid], { prepare: true }).then(result => {
      res.status(200).json(result.rows);
      console.log(results.rows);
    })
  } catch (err) {
    res.status(500).json({ message: res.message });
  }
});

module.exports = router;
