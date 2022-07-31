import "dotenv/config";

import express from "express";
import { default as pg } from "pg";
import cors from "cors";

const defaultPort = 3000;
const port = process.env.PORT ?? defaultPort;

const databaseConnectionPool = new pg.Pool({
    host: process.env.DATABASE_HOST,
    port: process.env.DATABASE_PORT,
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME,
});

const database = await databaseConnectionPool.connect();

const server = express();

server.use(cors());

server.get("/", async (request, response) =>
{
    const assetsQueryResult = await database.query("SELECT * FROM asset;");
    const assets = assetsQueryResult.rows;

    const tablesQueryResult = await database.query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name <> 'asset';");
    const tables = tablesQueryResult.rows.map((row) => row.table_name);

    const entries = [];

    for (const table of tables)
    {
        const tableQueryResult = await database.query(`SELECT * FROM ${table};`);
        entries.push(...tableQueryResult.rows.map((row) => ({ table, row })));
    }

    const contentfulLikeJson = {
        items: entries.map((entry) => ({
            sys: {
                id: entry.row.id,
                contentType: {
                    sys: {
                        id: entry.table,
                    },
                },
            },
            fields: entry.row,
        })),
        includes: {
            Asset: assets.map((asset) => ({
                sys: {
                    id: asset.id,
                },
                fields: {
                    title: asset.title,
                    file: {
                        url: `//${request.get('host')}/assets/${asset.id}`,
                        fileName: asset.name,
                    },
                },
            })),
        },
    };

    response.json(contentfulLikeJson);
});

server.get("/asset/:id", async (request, response) =>
{
    const assetQueryResult = await database.query("SELECT * FROM asset WHERE id = $1", [request.params.id]);

    if (assetQueryResult.rows.length === 0)
    {
        response.status(404).send("Asset not found!");
        return;
    }

    const asset = assetQueryResult.rows[0];

    response.contentType(asset.type);
    response.send(asset.data);
});

server.listen(port, () =>
{
    console.info(`Server started on port ${port}`);
});
