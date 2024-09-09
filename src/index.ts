import express from "express";
import { simpleFaker } from "@faker-js/faker";
import dayjs from "dayjs";
import { Database } from "bun:sqlite";

const app = express();

const db = new Database("db.sqlite", { create: true, strict: true });
db.exec(
	"CREATE TABLE Record (id TEXT PRIMARY KEY, type TEXT, subtype TEXT, reading INTEGER, location TEXT, timestamp TEXT);",
);
//db.exec("PRAGMA journal_mode = WAL;");

app.get("/", async (_, res) => {
	//const { rows } = await pool.query("SELECT 5 * 5 AS value");
	//res.send({ result: rows.at(0).value });
});

interface Record {
	id: string;
	type: string;
	subtype: string;
	reading: number;
	location: string;
	timestamp: string;
}

const createRecord = (): Record => {
	return {
		id: simpleFaker.string.uuid(),
		type: simpleFaker.string.uuid(),
		subtype: simpleFaker.string.uuid(),
		reading: simpleFaker.number.int({ min: 1, max: 1_000_000_000 }),
		location: simpleFaker.string.uuid(),
		timestamp: dayjs(
			simpleFaker.date.between({
				from: "2001-01-01",
				to: "2005-12-31",
			}),
		).format("YYYY-MM-DD HH:mm:ss"),
	};
};

app.get("/generate", async (req, res) => {
	let str = "id,type,subtype,reading,location,timestamp\n";
	for (let i = 0; i < Number(req.query.length ?? 5); i++) {
		const rec = createRecord();
		str += `${rec.id},${rec.type},${rec.subtype},${rec.reading},${rec.location},${rec.timestamp}\n`;
		if (i % 100000 === 0) console.log("here");
	}
	await Bun.write("./data.csv", str);
	res.send("ok");
});

app.listen(process.env.PORT ?? 3000);
