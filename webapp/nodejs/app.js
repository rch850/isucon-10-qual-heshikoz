"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const morgan_1 = __importDefault(require("morgan"));
const multer_1 = __importDefault(require("multer"));
const mysql_1 = __importDefault(require("mysql"));
const path_1 = __importDefault(require("path"));
const child_process_1 = __importDefault(require("child_process"));
const util_1 = __importDefault(require("util"));
const sync_1 = __importDefault(require("csv-parse/lib/sync"));
const camelcase_keys_1 = __importDefault(require("camelcase-keys"));
const ioredis_1 = __importDefault(require("ioredis"));
// require('newrelic');
const upload = multer_1.default();
const promisify = util_1.default.promisify;
const exec = promisify(child_process_1.default.exec);
const chairSearchCondition = require("../fixture/chair_condition.json");
const estateSearchCondition = require("../fixture/estate_condition.json");
const PORT = process.env.PORT ?? 1323;
const LIMIT = 20;
const NAZOTTE_LIMIT = 50;
const dbinfo = {
    host: process.env.MYSQL_HOST ?? "127.0.0.1",
    port: parseInt(process.env.MYSQL_PORT ?? "3306"),
    user: process.env.MYSQL_USER ?? "isucon",
    password: process.env.MYSQL_PASS ?? "isucon",
    database: process.env.MYSQL_DBNAME ?? "isuumo",
    connectionLimit: 10,
};
const redis = new ioredis_1.default(6379, '10.164.72.101');
const ESTATE_LOW_PRICE_CACHE = 'ESTATE_LOW_PRICE_CACHE';
const CHAIR_LOW_PRICE_CACHE = 'CHAIR_LOW_PRICE_CACHE';
const ESTATE_ID_CACHE = 'ESTATE_ID_CACHE';
const app = express_1.default();
const db = mysql_1.default.createPool(dbinfo);
app.set("db", db);
app.use(morgan_1.default("combined"));
app.use(express_1.default.json());
const botUA = [
    /ISUCONbot(-Mobile)?/,
    /ISUCONbot-Image\//,
    /Mediapartners-ISUCON/,
    /ISUCONCoffee/,
    /ISUCONFeedSeeker(Beta)?/,
    /crawler \(https:\/\/isucon\.invalid\/(support\/faq\/|help\/jp\/)/,
    /isubot/,
    /Isupider/,
    /Isupider(-image)?\+/,
    /(bot|crawler|spider)(?:[-_ .\/;@()]|$)/i,
];
app.use((req, res, next) => {
    const ua = req.header('User-Agent');
    if (ua && botUA.some(r => r.test(ua))) {
        res.status(503).send();
    }
    else {
        next();
    }
});
app.post("/initialize", async (req, res, next) => {
    try {
        await redis.del(ESTATE_LOW_PRICE_CACHE);
        await redis.del(CHAIR_LOW_PRICE_CACHE);
        const keys = await redis.keys(`${ESTATE_ID_CACHE}_*`);
        await Promise.all(keys.map(key => redis.del(key)));
        const dbdir = path_1.default.resolve("..", "mysql", "db");
        const dbfiles = [
            "0_Schema.sql",
            "1_DummyEstateData.sql",
            "2_DummyChairData.sql",
            "3_LatLon.sql",
        ];
        const execfiles = dbfiles.map((file) => path_1.default.join(dbdir, file));
        for (const execfile of execfiles) {
            await exec(`mysql -h ${dbinfo.host} -u ${dbinfo.user} -p${dbinfo.password} -P ${dbinfo.port} ${dbinfo.database} < ${execfile}`);
        }
        res.json({
            language: "nodejs",
        });
    }
    catch (e) {
        next(e);
    }
});
const estateQuery = `
  id,
  name,
  description,
  thumbnail,
  address,
  latitude,
  longitude,
  rent,
  door_height,
  door_width,
  features,
  popularity
`;
app.get("/api/estate/low_priced", async (req, res, next) => {
    const estatesCache = await redis.get(ESTATE_LOW_PRICE_CACHE);
    console.log(estatesCache);
    if (estatesCache) {
        res.header('Content-Type', 'application/json; charset=utf-8').send(estatesCache);
        return;
    }
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const es = await query(`SELECT ${estateQuery} FROM estate ORDER BY rent ASC, id ASC LIMIT ?`, [LIMIT]);
        const estates = es.map((estate) => camelcase_keys_1.default(estate));
        const json = JSON.stringify({ estates });
        await redis.set(ESTATE_LOW_PRICE_CACHE, json);
        res.header('Content-Type', 'application/json; charset=utf-8').send(json);
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/chair/low_priced", async (req, res, next) => {
    const chairsCache = await redis.get(CHAIR_LOW_PRICE_CACHE);
    if (chairsCache) {
        res.header('Content-Type', 'application/json; charset=utf-8').send(chairsCache);
        return;
    }
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const cs = await query("SELECT * FROM chair WHERE stock > 0 ORDER BY price ASC, id ASC LIMIT ?", [LIMIT]);
        const chairs = cs.map((chair) => camelcase_keys_1.default(chair));
        const json = JSON.stringify({ chairs });
        await redis.set(CHAIR_LOW_PRICE_CACHE, json);
        res.header('Content-Type', 'application/json; charset=utf-8').send(json);
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/chair/search", async (req, res, next) => {
    const searchQueries = [];
    const queryParams = [];
    const { priceRangeId, heightRangeId, widthRangeId, depthRangeId, kind, color, features, page, perPage, } = req.query;
    if (!!priceRangeId) {
        const chairPrice = chairSearchCondition["price"].ranges[priceRangeId];
        if (chairPrice == null) {
            res.status(400).send("priceRangeID invalid");
            return;
        }
        if (chairPrice.min !== -1) {
            searchQueries.push("price >= ? ");
            queryParams.push(chairPrice.min);
        }
        if (chairPrice.max !== -1) {
            searchQueries.push("price < ? ");
            queryParams.push(chairPrice.max);
        }
    }
    if (!!heightRangeId) {
        const chairHeight = chairSearchCondition["height"].ranges[heightRangeId];
        if (chairHeight == null) {
            res.status(400).send("heightRangeId invalid");
            return;
        }
        if (chairHeight.min !== -1) {
            searchQueries.push("height >= ? ");
            queryParams.push(chairHeight.min);
        }
        if (chairHeight.max !== -1) {
            searchQueries.push("height < ? ");
            queryParams.push(chairHeight.max);
        }
    }
    if (!!widthRangeId) {
        const chairWidth = chairSearchCondition["width"].ranges[widthRangeId];
        if (chairWidth == null) {
            res.status(400).send("widthRangeId invalid");
            return;
        }
        if (chairWidth.min !== -1) {
            searchQueries.push("width >= ? ");
            queryParams.push(chairWidth.min);
        }
        if (chairWidth.max !== -1) {
            searchQueries.push("width < ? ");
            queryParams.push(chairWidth.max);
        }
    }
    if (!!depthRangeId) {
        const chairDepth = chairSearchCondition["depth"].ranges[depthRangeId];
        if (chairDepth == null) {
            res.status(400).send("depthRangeId invalid");
            return;
        }
        if (chairDepth.min !== -1) {
            searchQueries.push("depth >= ? ");
            queryParams.push(chairDepth.min);
        }
        if (chairDepth.max !== -1) {
            searchQueries.push("depth < ? ");
            queryParams.push(chairDepth.max);
        }
    }
    if (!!kind) {
        searchQueries.push("kind = ? ");
        queryParams.push(kind);
    }
    if (!!color) {
        searchQueries.push("color = ? ");
        queryParams.push(color);
    }
    if (!!features) {
        const featureConditions = features.split(",");
        for (const featureCondition of featureConditions) {
            searchQueries.push("features LIKE CONCAT('%', ?, '%')");
            queryParams.push(featureCondition);
        }
    }
    if (searchQueries.length === 0) {
        res.status(400).send("Search condition not found");
        return;
    }
    searchQueries.push("stock > 0");
    if (!page || page != +page) {
        res.status(400).send(`page condition invalid ${page}`);
        return;
    }
    if (!perPage || perPage != +perPage) {
        res.status(400).send("perPage condition invalid");
        return;
    }
    const pageNum = parseInt(page, 10);
    const perPageNum = parseInt(perPage, 10);
    const sqlprefix = "SELECT * FROM chair WHERE ";
    const searchCondition = searchQueries.join(" AND ");
    const limitOffset = " ORDER BY popularity DESC, id ASC LIMIT ? OFFSET ?";
    const countprefix = "SELECT COUNT(*) as count FROM chair WHERE ";
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const jobs = await Promise.all([
            query(`${countprefix}${searchCondition}`, queryParams),
            query(`${sqlprefix}${searchCondition}${limitOffset}`, [...queryParams, perPageNum, perPageNum * pageNum])
        ]);
        const [{ count }] = jobs[0];
        const chairs = jobs[1];
        res.json({
            count,
            chairs: camelcase_keys_1.default(chairs),
        });
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/chair/search/condition", (req, res, next) => {
    res.json(chairSearchCondition);
});
app.get("/api/chair/:id", async (req, res, next) => {
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const id = req.params.id;
        const [chair] = await query("SELECT * FROM chair WHERE id = ?", [id]);
        if (chair == null || chair.stock <= 0) {
            res.status(404).send("Not Found");
            return;
        }
        res.json(camelcase_keys_1.default(chair));
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.post("/api/chair/buy/:id", async (req, res, next) => {
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const beginTransaction = promisify(connection.beginTransaction.bind(connection));
    const query = promisify(connection.query.bind(connection));
    const commit = promisify(connection.commit.bind(connection));
    const rollback = promisify(connection.rollback.bind(connection));
    try {
        const id = req.params.id;
        await beginTransaction();
        const [chair,] = await query("SELECT * FROM chair WHERE id = ? AND stock > 0 FOR UPDATE", [id]);
        if (chair == null) {
            res.status(404).send("Not Found");
            await rollback();
            return;
        }
        await query("UPDATE chair SET stock = ? WHERE id = ?", [
            chair.stock - 1,
            id,
        ]);
        await commit();
        if (chair.stock === 1) {
            await redis.del(CHAIR_LOW_PRICE_CACHE);
        }
        res.json({ ok: true });
    }
    catch (e) {
        await rollback();
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/estate/search", async (req, res, next) => {
    const searchQueries = [];
    const queryParams = [];
    const { doorHeightRangeId, doorWidthRangeId, rentRangeId, features, page, perPage, } = req.query;
    if (!!doorHeightRangeId) {
        const doorHeight = estateSearchCondition["doorHeight"].ranges[doorHeightRangeId];
        if (doorHeight == null) {
            res.status(400).send("doorHeightRangeId invalid");
            return;
        }
        if (doorHeight.min !== -1) {
            searchQueries.push("door_height >= ? ");
            queryParams.push(doorHeight.min);
        }
        if (doorHeight.max !== -1) {
            searchQueries.push("door_height < ? ");
            queryParams.push(doorHeight.max);
        }
    }
    if (!!doorWidthRangeId) {
        const doorWidth = estateSearchCondition["doorWidth"].ranges[doorWidthRangeId];
        if (doorWidth == null) {
            res.status(400).send("doorWidthRangeId invalid");
            return;
        }
        if (doorWidth.min !== -1) {
            searchQueries.push("door_width >= ? ");
            queryParams.push(doorWidth.min);
        }
        if (doorWidth.max !== -1) {
            searchQueries.push("door_width < ? ");
            queryParams.push(doorWidth.max);
        }
    }
    if (!!rentRangeId) {
        const rent = estateSearchCondition["rent"].ranges[rentRangeId];
        if (rent == null) {
            res.status(400).send("rentRangeId invalid");
            return;
        }
        if (rent.min !== -1) {
            searchQueries.push("rent >= ? ");
            queryParams.push(rent.min);
        }
        if (rent.max !== -1) {
            searchQueries.push("rent < ? ");
            queryParams.push(rent.max);
        }
    }
    if (!!features) {
        const featureConditions = features.split(",");
        for (const featureCondition of featureConditions) {
            searchQueries.push("features LIKE CONCAT('%', ?, '%')");
            queryParams.push(featureCondition);
        }
    }
    if (searchQueries.length === 0) {
        res.status(400).send("Search condition not found");
        return;
    }
    if (!page || page != +page) {
        res.status(400).send(`page condition invalid ${page}`);
        return;
    }
    if (!perPage || perPage != +perPage) {
        res.status(400).send("perPage condition invalid");
        return;
    }
    const pageNum = parseInt(page, 10);
    const perPageNum = parseInt(perPage, 10);
    const sqlprefix = `SELECT ${estateQuery} FROM estate WHERE `;
    const searchCondition = searchQueries.join(" AND ");
    const limitOffset = " ORDER BY popularity DESC, id ASC LIMIT ? OFFSET ?";
    const countprefix = "SELECT COUNT(id) as count FROM estate WHERE ";
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const jobs = await Promise.all([
            query(`${countprefix}${searchCondition}`, queryParams),
            query(`${sqlprefix}${searchCondition}${limitOffset}`, [...queryParams, perPageNum, perPageNum * pageNum]),
        ]);
        const [{ count }] = jobs[0];
        const estates = jobs[1];
        res.json({
            count,
            estates: camelcase_keys_1.default(estates),
        });
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/estate/search/condition", (req, res, next) => {
    res.json(estateSearchCondition);
});
app.post("/api/estate/req_doc/:id", async (req, res, next) => {
    const id = parseInt(req.params.id);
    if (id > 0 && id <= 29500) {
        res.json({ ok: true });
        return;
    }
    const hasCache = await redis.get(`${ESTATE_ID_CACHE}_${id}`);
    if (hasCache) {
        res.json({ ok: true });
        return;
    }
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const id = req.params.id;
        const [estate] = await query(`SELECT 1 FROM estate WHERE id = ?`, [id]);
        if (estate == null) {
            res.status(404).send("Not Found");
            return;
        }
        res.json({ ok: true });
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.post("/api/estate/nazotte", async (req, res, next) => {
    const coordinates = req.body.coordinates;
    const longitudes = coordinates.map((c) => c.longitude);
    const latitudes = coordinates.map((c) => c.latitude);
    const boundingbox = {
        topleft: {
            longitude: Math.min(...longitudes),
            latitude: Math.min(...latitudes),
        },
        bottomright: {
            longitude: Math.max(...longitudes),
            latitude: Math.max(...latitudes),
        },
    };
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const coordinatesToText = util_1.default.format("'POLYGON((%s))'", coordinates
            .map((coordinate) => util_1.default.format("%f %f", coordinate.latitude, coordinate.longitude))
            .join(","));
        const estatesInPolygon = await query(`SELECT ${estateQuery} FROM estate WHERE ST_Contains(ST_GeomFromText(${coordinatesToText}), latlon) ORDER BY popularity DESC, id ASC`, []);
        const results = {
            estates: [],
            count: 0,
        };
        let i = 0;
        for (const estate of estatesInPolygon) {
            if (i >= NAZOTTE_LIMIT) {
                break;
            }
            results.estates.push(camelcase_keys_1.default(estate));
            i++;
        }
        results.count = results.estates.length;
        res.json(results);
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/estate/:id", async (req, res, next) => {
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const id = req.params.id;
        const [estate] = await query(`SELECT ${estateQuery} FROM estate WHERE id = ?`, [id]);
        if (estate == null) {
            res.status(404).send("Not Found");
            return;
        }
        res.json(camelcase_keys_1.default(estate));
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.get("/api/recommended_estate/:id", async (req, res, next) => {
    const id = req.params.id;
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const query = promisify(connection.query.bind(connection));
    try {
        const [chair] = await query("SELECT * FROM chair WHERE id = ?", [id]);
        const w = chair.width;
        const h = chair.height;
        const d = chair.depth;
        const es = await query(`SELECT ${estateQuery} FROM estate where (door_width >= ? AND door_height>= ?) OR (door_width >= ? AND door_height>= ?) OR (door_width >= ? AND door_height>=?) OR (door_width >= ? AND door_height>=?) OR (door_width >= ? AND door_height>=?) OR (door_width >= ? AND door_height>=?) ORDER BY popularity DESC, id ASC LIMIT ?`, [w, h, w, d, h, w, h, d, d, w, d, h, LIMIT]);
        const estates = es.map((estate) => camelcase_keys_1.default(estate));
        res.json({ estates });
    }
    catch (e) {
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.post("/api/chair", upload.single("chairs"), async (req, res, next) => {
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const beginTransaction = promisify(connection.beginTransaction.bind(connection));
    const query = promisify(connection.query.bind(connection));
    const commit = promisify(connection.commit.bind(connection));
    const rollback = promisify(connection.rollback.bind(connection));
    try {
        await beginTransaction();
        const csv = sync_1.default(req.file.buffer, { skip_empty_lines: true });
        for (var i = 0; i < csv.length; i++) {
            const items = csv[i];
            await query("INSERT INTO chair(id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", items);
        }
        await commit();
        await redis.del(CHAIR_LOW_PRICE_CACHE);
        res.status(201);
        res.json({ ok: true });
    }
    catch (e) {
        await rollback();
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.post("/api/estate", upload.single("estates"), async (req, res, next) => {
    const getConnection = promisify(db.getConnection.bind(db));
    const connection = await getConnection();
    const beginTransaction = promisify(connection.beginTransaction.bind(connection));
    const query = promisify(connection.query.bind(connection));
    const commit = promisify(connection.commit.bind(connection));
    const rollback = promisify(connection.rollback.bind(connection));
    try {
        await beginTransaction();
        const csv = sync_1.default(req.file.buffer, { skip_empty_lines: true });
        for (var i = 0; i < csv.length; i++) {
            const items = csv[i];
            const latlon = `ST_GeomFromText('POINT(${items[5]} ${items[6]})')`;
            await query(`INSERT INTO estate(
            id,
            name,
            description,
            thumbnail,
            address,
            latitude,
            longitude,
            rent,
            door_height,
            door_width,
            features,
            popularity,
            latlon
          ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,${latlon}
          )`, items);
        }
        await commit();
        await Promise.all(csv.map((r) => redis.set(`${ESTATE_ID_CACHE}_${r[0]}`, '1')));
        await redis.del(ESTATE_LOW_PRICE_CACHE);
        res.status(201);
        res.json({ ok: true });
    }
    catch (e) {
        await rollback();
        next(e);
    }
    finally {
        await connection.release();
    }
});
app.listen(PORT, () => {
    console.log(`Listening ${PORT}`);
});
