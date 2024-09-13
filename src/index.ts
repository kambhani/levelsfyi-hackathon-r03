import express from "express";
import { simpleFaker } from "@faker-js/faker";
import dayjs from "dayjs";
import path from "node:path";
import axios from "axios";
import qs from "quickselect";
import { randomUUID } from "node:crypto";
import { open } from "lmdb";
import Papa from "papaparse";

const app = express();

const db = open({
	path: "db",
	compression: true,
	sharedStructuresKey: Symbol.for("structures"),
});

type Record = {
	id: string;
	type: string;
	subtype: string;
	reading: number;
	location: string;
	timestamp: string;
};

type CSVRecord = {
	id: string;
	type: string;
	subtype: string;
	reading: string;
	location: string;
	timestamp: string;
};

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
	}
	await Bun.write("./data.csv", str);
	res.send("ok");
});

app.get("/data", async (_, res) => {
	res.sendFile("data2.csv", { root: path.join(__dirname) });
});

app.post("/ingest", async (req, res) => {
	const start = new Date();
	const url = decodeURI(req.query.url as string);

	const response = await axios.get(url, {
		responseType: "stream",
	});

	Papa.parse(response.data, {
		fastMode: true,
		delimiter: ",",
		newline: "\n",
		header: true,
		step: async (row) => {
			await db.put(randomUUID(), {
				id: (row.data as CSVRecord).id,
				type: (row.data as CSVRecord).type,
				subtype: (row.data as CSVRecord).subtype,
				reading: Number.parseInt((row.data as CSVRecord).reading),
				location: (row.data as CSVRecord).location,
			});
		},
		complete: async () => {
			await db.flushed;
			return res.json({
				time: new Date().getTime() - start.getTime(),
			});
		},
	});
});

type RecordFilter = {
	id?: string[];
	type?: string[];
	subtype?: string[];
	location?: string[];
};

app.get("/median", async (req, res) => {
	const start = new Date();
	const filter = JSON.parse(
		decodeURI((req.query.filter as string | undefined) ?? "{}"),
	) as RecordFilter;

	const idSet = filter.id ? new Set(filter.id) : undefined;
	const typeSet = filter.type ? new Set(filter.type) : undefined;
	const subtypeSet = filter.subtype ? new Set(filter.subtype) : undefined;
	const locationSet = filter.location ? new Set(filter.location) : undefined;

	const readings: number[] = [];

	for (const { value } of db.getRange()) {
		if (idSet && !idSet.has(value.id)) continue;
		if (typeSet && !typeSet.has(value.type)) continue;
		if (subtypeSet && !subtypeSet.has(value.subtype)) continue;
		if (locationSet && !locationSet.has(value.location)) continue;
		readings.push(value.reading);
	}

	if (readings.length === 0) {
		return res.json({
			count: 0,
			median: 0,
		});
	}

	if (readings.length % 2 === 0) {
		qs(readings, (readings.length / 2) | 0);
		const upper = readings[(readings.length / 2) | 0];
		let lower = -1;
		for (let i = 0; i < readings.length / 2; i++)
			lower = Math.max(lower, readings[i]);
		console.log(new Date().getTime() - start.getTime());
		return res.json({
			count: readings.length,
			median: (upper + lower) / 2,
		});
	}

	qs(readings, (readings.length / 2) | 0);
	console.log(new Date().getTime() - start.getTime());
	return res.json({
		count: readings.length,
		median: readings[(readings.length / 2) | 0],
	});
});

app.get("/clear", async (_, res) => {
	await db.clearAsync();
	res.send("ok");
});

app.get("/exit", async (_, res) => {
	db.close();
	res.send("ok");
	process.exit(0);
});

app.listen(process.env.PORT ?? 5000);
