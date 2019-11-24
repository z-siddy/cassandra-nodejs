const express = require("express");
const app = express();

app.use(express.json());

const cassandraRouter = require("./routes/cassandra");
app.use("/cassandra", cassandraRouter);

app.listen(3000, () => console.log("Running on port 3000"));
