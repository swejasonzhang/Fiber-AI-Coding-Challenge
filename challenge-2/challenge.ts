/**
 * The entry point function. This will read the provided CSV file, scrape the companies'
 * YC pages, and output structured data in a JSON file.
 */

import { CheerioCrawler } from "crawlee";
import * as fsExtra from "fs-extra";
import * as csv from "fast-csv";
import fs from "fs";

interface Company {
  name: string;
  url: string;
}

interface Founder {
  name: string;
}

interface Job {
  title: string;
  location: string;
}

interface CompanyData {
  name: string;
  founded: string;
  description: string;
  teamSize: number;
  jobs: Job[];
  founders: Founder[];
}

// Parse CSV to extract company names and URLs
const parseCSV = async (filePath: string): Promise<Company[]> => {
  const companies: Company[] = []; // companies has to be an array
  return new Promise((resolve, reject) => {
    // return resolve if no errors, reject if there are
    fs.createReadStream(filePath) // read through information from file path
      .pipe(csv.parse({ headers: true })) // first key-value pair are the headers
      .on("data", (row) => {
        // Corrected object creation
        companies.push({ name: row["Company Name"], url: row["YC URL"] }); // push company name and URL
      })
      .on("end", () => resolve(companies)) // if no issues resolve the promise and return without any errors
      .on("error", (error) => reject(error)); // if there are errors then reject
  });
};

// Scrape company YC profile page
const scrapeCompanyPage = async (url: string): Promise<CompanyData> => {
  const companyData: CompanyData = await new Promise((resolve, reject) => {
    // return resolve if no errors, reject if there are
    const crawler = new CheerioCrawler({
      // initialize new CheerioCrawler to process information from HTML
      async requestHandler({ $, request }) {
        // $ is kinda like ${`company.name`} in JavaScript
        try {
          const name = $("h1.company-name").text().trim() || "N/A"; // Handle empty name
          const founded = $("span.founded").text().trim() || "N/A"; // Handle empty founded
          const description = $("p.description").text().trim() || "N/A"; // Handle empty description
          const teamSize = parseInt($("span.team-size").text(), 10) || 0; // if there is a team size, convert it into an int using parseInt

          const founders: Founder[] = []; // founders is an array of founder objects
          $("div.founders div.founder").each((_, el) => {
            // find all the divs with founder or founders text and loop through each one
            const founderName = $(el).find("h3").text().trim();
            if (founderName) {
              founders.push({ name: founderName }); // push the name of the founder as an object within the founders array
            }
          });

          const jobs: Job[] = []; // jobs is an array of job objects
          $("div.jobs div.job").each((_, el) => {
            // find all the divs with job or jobs text and loop through each one
            const title = $(el).find("h4").text().trim();
            const location = $(el).find("span.location").text().trim();
            if (title || location) {
              jobs.push({ title, location }); // push the title and location as an object into jobs
            }
          });

          resolve({ name, founded, description, teamSize, founders, jobs }); // signifies these attributes are all valid after the async operation
        } catch (error) {
          reject(error); // reject with the error
        }
      },
    });

    crawler.run([url]).catch(reject); // run the crawler and error handling to reject
  });

  return companyData; // return the data
};

// Write the JSON output to a file
const writeToFile = async (data: any, outputPath: string): Promise<void> => {
  // data could be any data type, outputPath has to be a string
  await fsExtra.ensureDir("out"); // make sure there is an out directory
  await fsExtra.writeJson(outputPath, data, { spaces: 2 }); // write in JSON to the outputPath
};

// Main function to process company list
export const processCompanyList = async (): Promise<void> => {
  // doesn't expect anything back because there's void
  const companies = await parseCSV("inputs/companies.csv"); // parsing all of the companies
  const companyDataPromises = companies.map((company) =>
    scrapeCompanyPage(company.url) // Use the URL here
  ); // scrape information from all of the companies
  const companyData = await Promise.all(companyDataPromises); // wait for the promises to be valid from all of the data

  await writeToFile(companyData, "out/scraped.json"); // Write scraped data to the JSON file
};

// Invoke the main function
processCompanyList().catch(console.error);