/**
 * The entry point function. This will download the given dump file, extract/decompress it,
 * parse the CSVs within, and add the data to a SQLite database.
 * This is the core function you'll need to edit, though you're encouraged to make helper
 * functions!
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";
import fs from "fs";
import https from "https";
import zlib from "zlib";
import tar from "tar";
import { parse } from "fast-csv";
import knex, { Knex } from "knex";
import { promises as fsPromises } from "fs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// helper function to download file using streams (ChatGPT)
const downloadFile = (url: string, destination: string): Promise<void> => {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destination); // create a writable stream to the specified destination file

    https
      .get(url, (response) => {
        // error handling
        if (response.statusCode !== 200) {
          reject(new Error(`Failed to get '${url}' (${response.statusCode})`));
          return;
        }
        
        // if successful, writes the response to the file.
        response.pipe(file);

        // once the data has been fully written.
        file.on("finish", () => {
          // Close the file
          file.close((err) => {
            // if there are any errors
            if (err) {
              reject(err); // don't complete the promise and return with errors
            } else {
              resolve(); // complete the promise without any errors
            }
          });
        });
      })
      // error handling
      .on("error", (err) => {
        // delete file at destination if errors occur
        fs.unlink(destination, (unlinkErr) => {
          if (unlinkErr) {
            reject(unlinkErr); // handle any errors while deleting the file
          } else {
            reject(err); // otherwise reject with the original error
          }
        });
      });
  });
};

// helper function to extract the .tar.gz file using streams
const extractTarGz = (source: string, destination: string): Promise<void> => {
  // source is where the data currently is, destination is where we want the data to go
  return new Promise((resolve, reject) => {
    fs.createReadStream(source) // reads the data within the source aka where the data is currently
      .pipe(zlib.createGunzip()) // reads the data but it also decompressed as well
      .pipe(tar.extract({ cwd: destination })) // extract the details of the .tar file and saves it into the destination file
      .on("finish", resolve) // if no errors, return and complete the promise
      .on("error", reject); // if errors, display errors
  });
};

// helper function to set up SQLite database with tables
const setupDatabase = async (dbPath: string) => {
  // initalize a new database using knex
  const db = knex({
    client: "sqlite3",
    connection: {
      filename: dbPath,
    },
    useNullAsDefault: true,
  });

  // if these tables exist, remove them

  await db.schema.dropTableIfExists("customers");
  await db.schema.dropTableIfExists("organizations");

  // create the tables
  await db.schema.createTable("customers", (table) => {
    table.increments("id").primary();
    table.string("name").notNullable();
    table.string("email").notNullable();
    table.string("phone").notNullable();
    table.string("address").notNullable();
    table.string("organization_id").notNullable();
  });

  await db.schema.createTable("organizations", (table) => {
    table.increments("id").primary();
    table.string("name").notNullable();
    table.string("industry").notNullable();
    table.string("address").notNullable();
  });

  return db;
};

// helper function to process CSV and insert data into the database in batches
const processCSV = (
  filePath: string,
  tableName: string,
  db: Knex
): Promise<void> => {
  return new Promise((resolve, reject) => {
    const rows: any[] = []; // initalize rows with the type of array
    const stream = fs.createReadStream(filePath); // create a read stream at the file path

    stream
      .pipe(parse({ headers: true })) // headers: true indicates that the first row of the file contain the headers and parse converts the object into key-value pairs
      .on("data", (row) => { // waits for the data event which happens every time a new row is parsed 
        rows.push(row);
        if (rows.length === 100) {
          // Batch insert for every 100 rows
          db(tableName).insert(rows).catch(reject); // inserts the current batch of rows on the specificed table name such as customers or organizations. if error then reject.
          rows.length = 0; // Clear rows after insert
        }
      })
      .on("end", async () => { // once the rows have been fully inserted
        // checking if there are more rows by chance
        if (rows.length > 0) {
          await db(tableName).insert(rows).catch(reject); // waits for more insertion of rows and if any errors arise then reject
        }
        resolve(); // complete the promise
      })
      .on("error", reject); // if error then reject
  });
};

// main function to process the data dump
export async function processDataDump() {
  try {
    // define file paths
    const tarGzPath = join(__dirname, "tmp", "dump.tar.gz");
    const extractPath = join(__dirname, "tmp");
    const dbPath = join(__dirname, "out", "database.sqlite"); 

    await extractTarGz(tarGzPath, extractPath);

    const extractedFiles = await fsPromises.readdir(extractPath);
    console.log("Extracted files:", extractedFiles);

    // download the .tar.gz file
    await downloadFile(
      "https://fiber-challenges.s3.amazonaws.com/dump.tar.gz",
      tarGzPath
    );

    // extract the .tar.gz file
    await extractTarGz(tarGzPath, extractPath);

    // set up SQLite database
    const db = await setupDatabase(dbPath);

    // process CSVs and insert into the database
    const customersCSV = join(extractPath, "customers.csv");
    const organizationsCSV = join(extractPath, "organizations.csv");
    await processCSV(customersCSV, "customers", db);
    await processCSV(organizationsCSV, "organizations", db);

    // confirm success
    console.log("Data successfully processed and stored in the database.");

    // Close the database connection
    await db.destroy(); // delete the database
  } catch (error) { // upon any errors, display the error
    console.error("Error processing data dump:", error);
  }
}
