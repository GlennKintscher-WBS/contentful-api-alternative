import "dotenv/config";

import { default as pg } from "pg";
import { default as inquirer } from "inquirer";
import contentful from "contentful";
import pgFormat from "pg-format";
import axios from "axios";

class ContentfulImporter
{
    #contentfulClient = null;
    #database = null;

    async import()
    {
        console.time("Import");

        console.info("Starting import…");

        try
        {
            const contentfulInformation = await ContentfulImporter.#askForContentfulInformation();
            this.#connectToContentful(contentfulInformation);

            const databaseInformation = await ContentfulImporter.#askForDatabaseInformation();
            await this.#connectToDatabase(databaseInformation);

            const contentfulModels = await this.#fetchModelsFromContentful();
            await this.#createDatabaseTables(contentfulModels);

            const contentfulEntries = await this.#fetchEntriesFromContentful();
            await this.#insertEntriesIntoDatabase(contentfulEntries);

            const contentfulAssets = await this.#fetchAssetsFromContentful();
            await this.#insertAssetsIntoDatabase(contentfulAssets);

            console.info("Import finished!");
        }
        catch (error)
        {
            console.error("Import failed!");
            throw error;
        }
        finally
        {
            this.#database?.end?.();
            console.timeEnd("Import");
        }
    }

    static async #askForContentfulInformation()
    {
        return await inquirer.prompt([
            {
                name: "spaceId",
                message: "What is your contentful space id?",
                default: process.env.CONTENTFUL_SPACE_ID,
            },
            {
                name: "accessToken",
                message: "What is your contentful access token?",
                default: process.env.CONTENTFUL_ACCESS_TOKEN,
            },
        ]);
    }

    static async #askForDatabaseInformation()
    {
        return await inquirer.prompt([
            {
                name: "host",
                message: "What is your database host?",
                default: process.env.DATABASE_HOST,
            },
            {
                name: "port",
                message: "What is your database port?",
                default: process.env.DATABASE_PORT,
            },
            {
                name: "user",
                message: "What is your database user?",
                default: process.env.DATABASE_USER,
            },
            {
                name: "password",
                message: "What is your database password?",
                default: process.env.DATABASE_PASSWORD,
            },
            {
                name: "database",
                message: "What is your database name?",
                default: process.env.DATABASE_NAME,
            },
        ]);
    }

    #connectToContentful(contentfulInformation)
    {
        console.info("Connecting to contentful…");

        this.#contentfulClient = contentful.createClient({
            space: contentfulInformation.spaceId,
            accessToken: contentfulInformation.accessToken,
        });

        console.debug("Connected to contentful!");
    }

    async #fetchFromContentful(fetchMethodName)
    {
        const limit = 1000;
        let skip = 0;
        let total = 0;

        const contentfulEntries = [];

        do
        {
            const contentfulCollection = await this.#contentfulClient[fetchMethodName]({
                order: "sys.createdAt",
                include: 0,
                limit,
                skip,
            });

            total = contentfulCollection.total;

            contentfulEntries.push(...contentfulCollection.items);
        }
        while (limit + skip < total);

        return contentfulEntries;
    }

    async #fetchModelsFromContentful()
    {
        console.info(`Fetching contentful models…`);

        const models = await this.#fetchFromContentful("getContentTypes");

        console.debug(`Fetched ${models.length} models from contentful!`);

        return models;
    }

    async #fetchAssetsFromContentful()
    {
        console.info(`Fetching contentful assets…`);

        const assets = await this.#fetchFromContentful("getAssets");

        console.debug(`Fetched ${assets.length} assets from contentful!`);

        return assets;
    }

    async #fetchEntriesFromContentful()
    {
        console.info(`Fetching contentful entries…`);

        const entries = await this.#fetchFromContentful("getEntries");

        console.debug(`Fetched ${entries.length} entries from contentful!`);

        return entries;
    }

    async #connectToDatabase(databaseInformation)
    {
        console.info("Connecting to database…");

        const connectionPool = new pg.Pool(databaseInformation);
        this.#database = await connectionPool.connect();

        console.debug("Connected to database!");
    }

    async #createDatabaseTables(contentfulModels)
    {
        console.info("Creating tables…");

        console.debug("Creating table for asset…");

        await this.#database.query(`
            DROP TABLE if EXISTS asset;
            CREATE TABLE if NOT EXISTS asset
            (
                id TEXT PRIMARY KEY,
                type TEXT,
                name TEXT,
                data BYTEA
            );
        `);

        for (const contentfulModel of contentfulModels)
        {
            const contentTypeIdentifier = pgFormat.ident(contentfulModel.sys.id);
            const contentTypeFieldDefinitions = contentfulModel.fields.map((field) => `${pgFormat.ident(field.id)} ${(ContentfulImporter.#getFieldType(field))}`);

            console.debug(`Creating table for ${contentTypeIdentifier} with ${contentTypeFieldDefinitions.length} columns…`);

            await this.#database.query(`
                DROP TABLE if EXISTS ${contentTypeIdentifier};
                CREATE TABLE if NOT EXISTS ${contentTypeIdentifier}
                (
                    id TEXT PRIMARY KEY,
                    ${contentTypeFieldDefinitions.join(", ")}
                );
            `);
        }

        console.debug(`Created ${contentfulModels.length} tables!`);
    }

    static #getFieldType(field)
    {
        const typeMap = {
            "Array": "JSON",
            "Boolean": "BOOLEAN",
            "Date": "DATE",
            "Integer": "INTEGER",
            "Link": "TEXT",
            "Number": "REAL",
            "Object": "JSON",
            "RichText": "TEXT",
            "Symbol": "TEXT",
            "Text": "TEXT",
        };

        return field.type === "Array" && field.items?.type ? `${typeMap[field.items.type]}[]` : typeMap[field.type];
    }

    async #insertAssetsIntoDatabase(contentfulAssets)
    {
        console.info("Inserting assets into database…");

        for (const contentfulAsset of contentfulAssets)
        {
            console.debug(`Inserting asset ${contentfulAsset.sys.id}…`);

            const fileResponse = await axios.get(`http:${contentfulAsset.fields.file.url}`, { responseType: "arraybuffer" });

            await this.#database.query(`
                INSERT INTO asset (id, type, name, data)
                VALUES ($1, $2, $3, $4)
            `, [
                contentfulAsset.sys.id,
                contentfulAsset.fields.file.contentType,
                contentfulAsset.fields.file.fileName,
                new Uint8Array(fileResponse.data),
            ]);
        }

        console.debug(`Inserted ${contentfulAssets.length} assets into database!`);
    }

    async #insertEntriesIntoDatabase(contentfulEntries)
    {
        console.info("Inserting entries into database…");

        for (const contentfulEntry of contentfulEntries)
        {
            const table = contentfulEntry.sys.contentType.sys.id;
            const id = contentfulEntry.sys.id;

            console.debug(`Inserting entry ${id} into table ${table}…`);

            const columns = [];
            const values = [];

            for (const [columnName, value] of Object.entries(contentfulEntry.fields))
            {
                columns.push(pgFormat.ident(columnName));

                if (Array.isArray(value))
                {
                    values.push(`ARRAY[${pgFormat.literal(value.map((element) => element.sys?.id ?? element))}]`);
                }
                else
                {
                    values.push(pgFormat.literal(value.sys?.id ?? value));
                }
            }

            await this.#database.query(`
                INSERT INTO ${pgFormat.ident(table)} (id, ${columns.join(", ")})
                VALUES (${pgFormat.literal(id)}, ${values.join(", ")})
            `);
        }

        console.debug(`Inserted ${contentfulEntries.length} entries into database!`);
    }
}

const contentfulImporter = new ContentfulImporter();
await contentfulImporter.import();
