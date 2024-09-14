Sentry.init({
  dsn: 'redacted',
  tracesSampleRate: 1.0,
});

const MAX_RETRIES = 3;
let CONCURRENCY_LIMIT = 20;
const RETRY_DELAY_BASE = 100; // Base delay in ms for exponential backoff
const EMPTY_STRING = '';
const UNKNOWN = 'Unknown';
let MODE = 'trial';
let CUSTOMER_NAME = '';
let CUSTOMER_EMAIL = '';

function createCsvMessageForUser(emptyColumns, firstName) {
  const emptyCell = '"",';
  const emptyCells = emptyCell.repeat(emptyColumns);
  let message = "\n";
  if (MODE === 'trial') {
    message += `${emptyCells}"You can buy a license (starting at $1) to get your full book collection:"\n`;
    message += `${emptyCells}"https://bit.ly/amazon-book-list-exporter-license"\n\n`;
  } else { // full access
    message += `${emptyCells}"Hi ${firstName}, you can give me suggestions at app.maker.supreme@gmail.com"\n`;
    message += `${emptyCells}"If this looks good, could you leave me a review please? The link is right below: "\n`;
    message += `${emptyCells}"${EXTENSION_URL}"\n\n`;
  }
  return message;
}

function escapeDoubleQuotes(input) {
  return input.replace(/"/g, '""');
}

// log error and the data object causing the error
function logErrorDetails(error, rawData) {
  const errorDetails = {
    customerName: CUSTOMER_NAME,
    customerEmail: CUSTOMER_EMAIL,
    message: error.message,
    name: error.name,
    stack: error.stack ? error.stack.split('\n') : '',
    additionalData: error.additionalData,
    rawData: JSON.stringify(rawData, null, 2) || 'Raw data not provided',
  };

  if (error instanceof ErrorEvent) {
    // ErrorEvent objects have additional properties that might be useful.
    errorDetails.lineno = error.lineno;
    errorDetails.colno = error.colno;
    errorDetails.filename = error.filename;
  } else if (error instanceof DOMException) {
    // DOMException objects have a `code` property representing the error code.
    errorDetails.code = error.code;
  }

  // Add any custom properties that you know are sometimes added to errors in your context
  // ...

  console.error('Error details:', errorDetails);

  // Send a message to Sentry with the serialized error details
  Sentry.captureMessage(`Error: ${errorDetails.message}`, {
    level: 'error', // Set the level to 'error'
    extra: errorDetails, // Attach the error details as extra data
  });
}

async function fetchWithRetry(url, maxRetries, retryCount = 0) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error('Fetch failed with status: ' + response.status);
    }
    return response;
  } catch (error) {
    if (retryCount < maxRetries) {
      const delay = RETRY_DELAY_BASE * Math.pow(2, retryCount);
      await new Promise((resolve) => setTimeout(resolve, delay));
      return fetchWithRetry(url, maxRetries, retryCount + 1);
    } else {
      logErrorDetails(error);
    }
  }
}

async function getBookListWithOrder(domain, sortOrder, trialMode = false) {
  let paginationToken = null;
  const books = [];
  let bookCount = 0;

  while (true) {
    // trial mode, fetch 1 batch of books then stop
    if (trialMode && books.length > 0) {
      break;
    }
    let querySize = 50;
    if (trialMode) {
      querySize = 10;
    }
    const url = `https://${domain}/kindle-library/search?query=&libraryType=BOOKS${paginationToken ? '&paginationToken=' + paginationToken : ''
      }&sortType=${sortOrder}&querySize=${querySize}`;
    // each fetch takes ~430ms, for maximum 50 books (query size)
    // all other processing takes negligible time
    let response;
    try {
      response = await fetchWithRetry(url, MAX_RETRIES);
      const data = await response.json();
      if (!data || typeof data !== 'object' || !Array.isArray(data.itemsList)) {
        throw new Error('Invalid data structure');
      }

      for (const book of data.itemsList) {
        const bookDetails = {
          asin: book.asin || UNKNOWN,
          title: book.title || UNKNOWN,
          originType: book.originType?.toLowerCase() || UNKNOWN,
          resourceType: book.resourceType?.toLowerCase() || UNKNOWN,
          authors: book.authors && book.authors[0] ? book.authors[0] : UNKNOWN,
        };
        books.push(bookDetails);
      }

      bookCount += data.itemsList.length;

      paginationToken = data.paginationToken || '';
      // todo: when fetch books 2 ways, bookCount go up then go up again from 0
      // could be confusing
      chrome.runtime.sendMessage({
        action: 'updateOverallBookCount',
        count: bookCount,
      });
      if (!paginationToken) {
        break;
      }
    } catch (error) {
      logErrorDetails(error, data);
      break;
    }
  }
  return books;
}

function mergeBookLists(booksAsc, booksDesc) {
  const bookMap = new Map();

  // Add books from the ascending list
  booksAsc.forEach((book) => bookMap.set(book.asin, book));

  // Add books from the descending list, avoiding duplicates
  booksDesc.forEach((book) => {
    if (!bookMap.has(book.asin)) {
      bookMap.set(book.asin, book);
    }
  });

  // Convert the map back to an array
  const mergedBooks = Array.from(bookMap.values());
  return mergedBooks;
}

// iteratively keep querying for books (most recently purchased first)
// until there's no more (i.e. no more pagination token)
async function getBookList(domain) {
  console.log({ MODE });
  const booksAsc = await getBookListWithOrder(domain, 'acquisition_asc',
   /* trialMode */ MODE === 'trial');
  // assuming each book order can fetch at most 10k books
  if (booksAsc.length < 9999) {
    // trial mode will stop here also
    return booksAsc;
  }
  const booksDesc = await getBookListWithOrder(domain, 'acquisition_desc');
  // trying to fetch more than 10k books by fetching books in 2 orders
  const allBooks = mergeBookLists(booksAsc, booksDesc);
  chrome.runtime.sendMessage({
    action: 'updateOverallBookCount',
    count: allBooks.length,
  });
  return allBooks;
}

// authors field of a book has bad formatting. We need to
// convert "Detroja, Parth:Agashe, Aditya:Mehta, Neel:Detroja, Parth:Agashe, Aditya:Mehta, Neel:"
// to "Parth Detroja, Agashe Aditya, Neel Mehta"
function extractAuthors(rawAuthors) {
  if (typeof rawAuthors !== 'string' || rawAuthors.length === 0) {
    logErrorDetails(new Error('Unknown author'), rawAuthors);
    return UNKNOWN;
  }

  // Normalize delimiter inconsistencies (if any), then split
  // replace ", " with ","
  const authorsArray = rawAuthors
    .replace(/, /g, ',')
    .split(':')
    .filter(Boolean);
  const uniqueAuthors = [...new Set(authorsArray)];

  const formattedAuthors = uniqueAuthors
    .map((author) => {
      const nameParts = author.split(',');
      if (nameParts.length === 2) {
        // e.g. "Gray, Nick", needs to swap name order
        const [lastName, firstName] = nameParts;
        return `${firstName.trim()} ${lastName.trim()}`;
      } else if (nameParts.length === 1) {
        // e.g. "Nick Maggiulli:", "Keenan", don't do anything
        return nameParts[0].trim();
      } else {
        // Unexpected format, use raw format
        return author;
      }
    })
    .join(', ');
  return formattedAuthors;
}

// Listen for a message from popup
// popup.js will trigger this section
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'getBookList') {
    MODE = request.mode;
    CUSTOMER_NAME = request.customerName;
    CUSTOMER_EMAIL = request.customerEmail;
    const firstName = getFirstName(CUSTOMER_NAME);
    getBookList(request.domain)
      .then((books) => {
        let csvData = '';
        // aligns texts to Title column to look good
        csvData += createCsvMessageForUser(4, firstName);
        // column names
        csvData += `"ISBN / ASIN (Amazon ID)","Link","Type","Origin","Title","Author(s)"\n`;
        for (const book of books) {
          try {
            const formattedAuthors = extractAuthors(book.authors);
            // "${formattedAuthors}" is treated as a single field
            // even if within it there are commas
            // e.g. "ASIN1","Title1","Author1, Author2, Author3"
            csvData +=
              `="${book.asin}"` +
              `,"${'https://www.amazon.com/dp/' + book.asin}"` +
              `,"${book.resourceType}"` +
              `,"${book.originType}"` +
              `,"${escapeDoubleQuotes(book.title)}"` +
              `,"${formattedAuthors}"\n`;
          } catch (error) {
            logErrorDetails(error, book);
          }
        }
        // aligns texts to Title column to look good
        csvData += createCsvMessageForUser(4, firstName);
        chrome.runtime.sendMessage({
          action: 'done',
        });

        sendResponse({ csv: csvData });
      })
      .catch((error) => {
        logErrorDetails(error);
      });
    return true; // Enables async sendResponse
  }
});

function createQuery(quantity, bookTypeUuid, endCursor, asinOnly = false) {
  const nodeFields = asinOnly ?
    `asin` :
    `asin
         relationshipType
         relationshipSubType
         relationshipCreationDate
         product {
             asin
             title {
                 displayString
             }
             byLine {
                 contributors {
                     name
                 }
             }
         }`;

  return `
    query ccGetCustomerLibraryBooks {
        getCustomerLibrary {
            books(
                after: "${endCursor}"
                first: ${quantity}
                sortBy: { sortField: ACQUISITION_DATE, sortOrder: DESCENDING }
                selectionCriteria: {
                    tags: []
                    query: "${bookTypeUuid}"
                }
            ) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        ${nodeFields}
                    }
                }
            }
        }
    }`;
}

function createQueryForBookSeries(keyword, genre, endCursor) {
  return `
    query ccGroupQuery {
        getCustomerLibrary {
            seriesAggregation(first: 100, after: "${endCursor}",
                libraryType: OWNED,
                searchCriteria: {keyword: "${keyword}"},
                selectionCriteria: {tags: [], query: "${genre}"}) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                totalCount {
                    number
                }
                edges {
                    node {
                        asin
                    }
                }
            }
        }
    }`;
}

function createQueryForBookGenres(endCursor) {
  return `
    query ccGroupQuery {
        getCustomerLibrary {
            genreAggregation(first: 50,
                after: "${endCursor}",
                libraryType: OWNED,
                selectionCriteria: {tags: [], query: ""}) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        name
                        subGenre {
                          id
                          name
                        }
                    }
                }
            }
        }
    }`;
}

function createQueryForBooksInGenre(genreId, endCursor, subGenreId = "") {
  return `
    query ccSingleGroupAsinQuery {
        getCustomerLibrary {
            genre(genreId: "${genreId}", libraryType: OWNED) {
                id
                name
                books(after: "${endCursor}",
                first: 300,
                selectionCriteria: {tags: [], query: "${subGenreId}"}) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    edges {
                        node {
                            asin
                        }
                    }
                    __typename
                }
            }
        }
    }
    `;
}

function createQueryForBooksInSeries(seriesId, endCursor) {
  return `
    query ccSingleGroupAsinQuery {
        getCustomerLibrary {
            series(seriesId: "${seriesId}", libraryType: OWNED) {
                product {
                    asin
                    title {
                        displayString
                    }
                }
                books(after: "${endCursor}", first: 50, selectionCriteria: {tags: [], query: ""}) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    edges {
                        node {
                            asin
                        }
                    }
                }
            }
        }
    }`;
}

function getReadableAcquiredDate(node) {
  if (!node || !node.relationshipCreationDate) {
    logErrorDetails(new Error('Unknown date'), node);
    return EMPTY_STRING;
  }
  const acquiredTimestamp = node.relationshipCreationDate; // unix
  // Parsing the timestamp and checking if the result is a valid date
  const acquiredDate = new Date(parseInt(acquiredTimestamp, 10));
  if (isNaN(acquiredDate.getTime())) {
    logErrorDetails(new Error('invalid date'), node);
    return 'Invalid Date';
  }
  // todo: make timezone/location a variable
  return acquiredDate.toLocaleDateString('en-US', {
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  });
}

// to avoid POST net::ERR_INSUFFICIENT_RESOURCES error
async function limitConcurrency(tasks, limit) {
  console.log({ CONCURRENCY_LIMIT });
  let activePromises = [];
  const results = [];
  for (const task of tasks) {
    // Start task and add its promise to the active list
    const promise = task()
      .then((result) => {
        results.push(result);
        // Remove this promise from the active list once it resolves
        activePromises = activePromises.filter((p) => p !== promise);
      })
      .catch((error) => {
        // Remove this promise from the active list if it rejects
        activePromises = activePromises.filter((p) => p !== promise);
        logErrorDetails(error);
      });
    activePromises.push(promise);

    // If we've reached the concurrency limit, wait for one promise to finish
    if (activePromises.length >= limit) {
      await Promise.race(activePromises);
    }
  }
  // Wait for all remaining promises to finish
  await Promise.all(activePromises);
  return results;
}

// returns array
// always have at least 1 author which is "Unknown"
function getBookAuthors(node) {
  /*
    // doesn't handle quotes well, authors got split to multiple columns
    // unintentionally. haven't found a good way to solve without messing up
    // other cases yet.
    "contributors": [
        {
          "name": "Curtis \"50 Cent\" Jackson"
        },
        {
          "name": "Curtis 50 Cent Jackson"
        },
        {
          "name": "HarperAudio"
        }
      ]
    */
  const contributors = node?.product?.byLine?.contributors ?? EMPTY_STRING;
  if (contributors === EMPTY_STRING) {
    // logErrorDetails(new Error('No author'), node);
    return [UNKNOWN];
  }

  const bookAuthors = [];

  for (const contributor of contributors) {
    if (contributor.name) {
      // append name to array bookAuthors
      const cleanedName = contributor.name.trim().replace(/\s+/g, ' ');
      // escape double quotes i.e. "name": "Curtis \"50 Cent\" Jackson"
      bookAuthors.push(escapeDoubleQuotes(cleanedName));
    }
  }
  return bookAuthors;
}

function createQueryProductDetails(asinList) {
  const asinInputs = asinList.map(asin => `{asin: "${asin}"}`).join(',');
  return `
  query ccGetProductQuickView {
    getProducts(input: [
      ${asinInputs}
    ]) {
      asin
      overview {
        sectionGroups {
          name {
            id
          }
          sections {
            attributes {
              granularizedValue {
                displayContent
              }
              label {
                displayContent
                id
              }
            }
          }
        }
      }
      bookSeries {
        singleBookView {
          series {
            title
            position
          }
        }
      }
      buyingOptions {
        options {
          price {
            priceToPay {
              moneyValueOrRange {
                value {
                  amount
                  currencyCode
                }
              }
            }
          }
        }
      }
    }
  }
  `;
}

function getBookDetails(responseJson) {
  const products = responseJson?.data?.getProducts || [];

  const productDetailsArray = [];
  for (const product of products) {
    const details = {
      asin: product?.asin,
      numberOfPages: null,
      listeningLength: null,
      seriesTitle: null,
      seriesPosition: null,
      detailedFormat: null
    };

    const sectionGroups = product?.overview?.sectionGroups || [];
    for (const group of sectionGroups) {
      const sections = group?.sections || [];
      for (const section of sections) {
        const attributes = section?.attributes || [];
        for (const attribute of attributes) {
          const labelId = attribute?.label?.id;
          const granularizedValueDisplayContent = attribute?.granularizedValue?.displayContent;
          const labelDisplayContent = attribute?.label?.displayContent;
          if (labelId === "book_details-fiona_pages" && details.numberOfPages === null) {
            details.numberOfPages = granularizedValueDisplayContent?.fragments?.[0]?.text || null;
          } else if (labelId === "book_details-binding" && details.detailedFormat === null) {
            details.detailedFormat = labelDisplayContent?.fragments?.[0]?.text || null;
          } else if (labelId === "audiobook_details-listening_length" && details.listeningLength === null) {
            details.listeningLength = granularizedValueDisplayContent?.fragments?.[0]?.text || null;
          }
        }
      }
    }

    const bookSeries = product?.bookSeries?.singleBookView?.series;
    if (bookSeries) {
      details.seriesTitle = bookSeries.title || null;
      details.seriesPosition = bookSeries.position || null;
    }

    productDetailsArray.push(details);
  }
  return productDetailsArray;
}

// simulate clicking on each book to get more details
// todo: update progress for this step
async function addMiscDetails(bookMapByAsin) {
  const csrfTokenMeta = document.querySelector(
    'meta[name="anti-csrftoken-a2z"][id="kindle-reader-api"]',
  );
  const csrfToken = csrfTokenMeta ?
    csrfTokenMeta.getAttribute('content') :
    null;

  const asinList = Array.from(bookMapByAsin.keys());
  const batchSize = 30; // max batch size is 30, anymore and api will throw error
  const fetchTasks = [];
  const queryStartTime = new Date();
  for (let i = 0; i < asinList.length; i += batchSize) {
    const batchAsinList = asinList.slice(i, i + batchSize); // i + batchSize > asinList.length is fine
    const query = createQueryProductDetails(batchAsinList);

    const task = async () => {
      const maxAttempts = 3;
      let attempt = 0;
      while (attempt < maxAttempts) {
        try {
          const response = await fetch('https://www.amazon.com/kindle-reader-api', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Anti-Csrftoken-A2z': csrfToken,
              // without this header below some books will have null price
              // but with this header fetch failure rate goes up (from 0% to ~35% fail)
              // todo: only include this header for books that have null price
              // 'X-Is-Detail-Page-Context': true
            },
            body: JSON.stringify({
              query,
              operationName: "ccGetProductQuickView"
            }),
          });
          const text = await response.text();
          const responseJson = JSON.parse(text);
          // todo: handle errors better
          if (responseJson.errors) {
            throw new Error(JSON.stringify(responseJson.errors.extensions, null, 2));
          }
          return { text, status: response.status };
        } catch (error) {
          attempt++;
          if (attempt >= maxAttempts) {
            // console.log("attempt", attempt);
            // console.log(error);
            // logErrorDetails(error); // only log when exhausting retries
            return { error, status: 'network failure' };
          }
          const backoffTime = Math.pow(2, attempt) * 100; // Exponential backoff formula
          await new Promise((resolve) => setTimeout(resolve, backoffTime));
        }
      }
    };
    fetchTasks.push(task);
  }

  const responses = await limitConcurrency(fetchTasks, CONCURRENCY_LIMIT);
  // console.log(
  //   `query duration: ${(new Date() - queryStartTime) / 1000
  //   } seconds`,
  // );
  const extractDetailsStartTime = new Date();
  for (const { text, status, error } of responses) {
    if (status === 'network failure' || status !== 200) {
      console.log('Fetching product details failed:', JSON.stringify(error, null, 2));
      continue;
    }
    const responseJson = JSON.parse(text);
    // console.log(responseJson);
    const productDetailsArray = getBookDetails(responseJson);
    for (const { asin, price, numberOfPages, listeningLength, seriesTitle, seriesPosition, detailedFormat } of productDetailsArray) {
      const book = bookMapByAsin.get(asin);
      if (book) {
        // "123 pages" -> 123
        book.numberOfPages = numberOfPages ? parseInt(numberOfPages.split(' ')[0]) : null;
        book.listeningLength = listeningLength;
        book.seriesTitle = seriesTitle;
        book.seriesPosition = seriesPosition;
        // only matters when detailedFormat is "Mass Market Paperback"
        if (!book.format) {
          book.format = detailedFormat;
        }
      }
    }
  }
  // console.log(
  //   `extractDetails duration: ${(new Date() - extractDetailsStartTime) / 1000
  //   } seconds`,
  // );
  // console.log(`Fetched product details for ${bookDetailCount} books`);
}

// todo: maybe get format from getBookDetails
async function addBookFormat(bookMapByAsin) {
  let bookCount = 0;
  // from looking at Initiator tab (under Network tab) for kindle-reader-api request to server
  // and searching the relevant js file for the uuid corresponding to kindle book type:
  // there are many more uuids, not sure what they mean
  const bookTypeUuidMap = {
    'Paperback': '9504bfd1def00d211775cbed8234df6b',
    'Kindle eBook': '549b56ec07dceb02eb010bbde91e654f',
    'Hardcover': '6c354d115d06b99ae435776ce1eb971e',
    'Audible Audiobook': 'db972bb474323d2d9fbb7bc828c5814a',
    'Board Book': '4f5d5a055115ae14c88c7bbc051d7d5b',
  };
  for (const [bookType, bookTypeUuid] of Object.entries(bookTypeUuidMap)) {
    const createQueryForGetBookFormat = function (endCursor) {
      // just need to add asin, to add bookType to overall book info below
      return createQuery(MODE === 'trial' ? 5 : 300, bookTypeUuid, endCursor,  /* asinOnly */ MODE === 'trial' ? false : true);
    };
    const getPageInfoFn = function (responseJson) {
      return responseJson?.data?.getCustomerLibrary?.books?.pageInfo;
    };
    const allFetchedData = await fetchAllData(
      createQueryForGetBookFormat,
      getPageInfoFn,
      MODE === 'trial'
    );

    for (const responseJson of allFetchedData) {
      // 1 edge = 1 book
      const edges = responseJson?.data?.getCustomerLibrary?.books?.edges ?? [];
      for (const edge of edges) {
        try {
          const node = edge.node;
          if (!node || !node.asin) {
            logErrorDetails(new Error('No node or node asin'), node);
            continue;
          }
          if (MODE === 'full access') {
            // just need to add format to the book identified by asin
            const book = bookMapByAsin.get(node.asin);
            if (book) {
              book.format = bookType;
              bookCount += 1;
            }
          } else if (MODE === 'trial') {
            // do similar work as getBookMapByAsin in full access mode
            // and adds format also
            const authors = getBookAuthors(node);
            const book = {
              asin: node.asin,
              format: bookType,
              title: node?.product?.title?.displayString ?? EMPTY_STRING,
              acquiredDate: getReadableAcquiredDate(node),
              // Defensive programming to handle undefined, null
              relationshipSubType: (node.relationshipSubType || []).join(', '),
              relationshipType: node.relationshipType.toLowerCase() ?? EMPTY_STRING,
              authors: authors,
            };

            if (!bookMapByAsin.has(book.asin)) {
              bookMapByAsin.set(book.asin, book);
              bookCount += 1;
            }
          }
        } catch (error) {
          logErrorDetails(error, edge);
        }
      }
      chrome.runtime.sendMessage({
        action: 'updateBookFormatCount',
        count: bookCount,
      });
    }
  }
}

async function assignSeriesToBooks(bookMapByAsin, seriesIds, bookCount) {
  const fetchTasks = [];
  for (const seriesId of seriesIds) {
    const task = async () => {
      const maxAttempts = 3;
      let attempt = 0;
      while (attempt < maxAttempts) {
        try {
          const allBooksInOneSeries = await fetchAllData(
            (endCursor) => createQueryForBooksInSeries(seriesId, endCursor),
            (responseJson) =>
              responseJson?.data?.getCustomerLibrary?.series?.books?.pageInfo,
          );
          // console.log(`Fetch all books in series: ${seriesId}`);
          return allBooksInOneSeries;
        } catch (e) {
          console.log(`series ${seriesId}, attempt ${attempt} failed: `, e);
          attempt++;
          if (attempt >= maxAttempts) {
            logErrorDetails(e); // only log when exhausting retries
            return []; // Return an empty array after exhausting retries
          }
          const backoffTime = Math.pow(2, attempt) * 100; // Exponential backoff formula
          await new Promise((resolve) => setTimeout(resolve, backoffTime));
        }
      }
    };
    fetchTasks.push(task);
  }

  const fetchedResults = await limitConcurrency(fetchTasks, CONCURRENCY_LIMIT);

  // Flatten the results and filter out empty responses due to errors
  const booksPerSeries = fetchedResults
    .flat()
    .filter((result) => result.length !== 0);

  // then assign series name to each book in bookMapByAsin
  for (const responseJson of booksPerSeries) {
    // todo: deprecate seriesName, already use seriesTitle from detailed book info
    const seriesName =
      responseJson?.data?.getCustomerLibrary?.series?.product?.title
        ?.displayString;
    if (!seriesName) {
      continue;
    }
    // 1 edge = 1 book = 1 node
    const edges = responseJson?.data?.getCustomerLibrary?.series?.books?.edges;
    if (!edges) {
      continue;
    }

    for (const edge of edges) {
      const asin = edge?.node?.asin;
      const book = bookMapByAsin.get(asin);
      // add seriesName to a book object
      if (book) {
        book.seriesName = escapeDoubleQuotes(seriesName);
        bookCount += 1;
      }
    }
    chrome.runtime.sendMessage({
      action: 'updateBookSeriesCount',
      count: bookCount,
    });
  }
  return bookCount;
}

// get the series each book belong to, if available
// todo: get book series from addMiscDetails step. cleaner that way
async function addBooksSeries(genres, bookMapByAsin) {
  let bookCount = 0;
  // each query for series can only get 300 series max across all pages
  // so further filter each query for series by genre helps get more series overall

  const fetchBookSeriesPerGenreTasks = [];

  const fetchBookSeriesAllGenresStartTime = new Date();
  // console.log(`getting book series for ${genres.length} genres`);
  for (const { id } of genres) {
    const task = async () => {
      try {
        const allFetchedDataForGenre = await fetchAllData(
          (endCursor) => createQueryForBookSeries('', id, endCursor),
          (responseJson) =>
            responseJson?.data?.getCustomerLibrary?.seriesAggregation?.pageInfo,
        );
        return allFetchedDataForGenre;
      } catch (error) {
        logErrorDetails(error);
        return []; // Return an empty array in case of error to maintain structure
      }
    };
    fetchBookSeriesPerGenreTasks.push(task);
  }
  // Wait for all fetch operations to complete
  const fetchedResults = await limitConcurrency(
    fetchBookSeriesPerGenreTasks,
    CONCURRENCY_LIMIT,
  );
  // Flatten the results and filter out empty responses due to errors
  const allFetchedData = fetchedResults
    .flat()
    .filter((result) => result.length !== 0);

  console.log(
    `fetchBookSeriesAllGenres duration: ${(new Date() - fetchBookSeriesAllGenresStartTime) / 1000
    } seconds`,
  );

  // from looking at Initiator tab (under Network tab) for kindle-reader-api request to server
  // and searching the relevant js file for the uuid corresponding to kindle book type:
  // there are many more uuids, not sure what they mean
  // get all books from all pages
  // because genres overlapp, so need to dedup series across genres
  const seriesIds = new Set();
  for (const responseJson of allFetchedData) {
    const seriesAggregation =
      responseJson?.data?.getCustomerLibrary?.seriesAggregation;
    // 1 edge = 1 series = 1 node
    const edges = seriesAggregation?.edges ?? [];
    for (const edge of edges) {
      const seriesId = edge?.node?.asin;
      if (seriesId) {
        seriesIds.add(seriesId);
      }
    }
  }

  const assignSeriesToBooksRound1StartTime = new Date();
  bookCount = await assignSeriesToBooks(bookMapByAsin, seriesIds, bookCount);
  console.log(
    `assignSeriesToBooksRound1 duration: ${(new Date() - assignSeriesToBooksRound1StartTime) / 1000
    } seconds`,
  );
  // for books without series, search using their author as keywords
  // to see if there's any series by that author
  const authorsForBooksWithoutSeries = new Set();
  for (const book of bookMapByAsin.values()) {
    if (!book.seriesName && book.authors && book.authors.length > 0) {
      const firstAuthor = book.authors[0];
      authorsForBooksWithoutSeries.add(firstAuthor);
    }
  }
  console.log(
    `getting book series for ${authorsForBooksWithoutSeries.size} authors`,
  );

  const fetchTasks = [];

  const fetchBookSeriesAllAuthorsStartTime = new Date();
  const authorsArray = Array.from(authorsForBooksWithoutSeries);
  // filter out elements with "Curtis" in authorsArray,
  // unhandled escaped chars causes failure
  // authorsArray = authorsArray.filter(author => !author.includes("Curtis"));
  // const duplicateArray = (arr, times) => [
  //     ...Array.from({ length: times }, () => [...arr]).flat()
  // ];

  // const duplicatedAuthorsArray = duplicateArray(authorsArray, 20);

  // console.log(`getting book series for ${duplicatedAuthorsArray.length} authors`);
  // each query for series can only get 300 series max across all pages
  // so further filter each query for series by genre helps get more series overall
  for (const author of authorsArray) {
    const task = async () => {
      const maxAttempts = 3;
      let attempt = 0;
      while (attempt < maxAttempts) {
        try {
          const allFetchedDataForAuthor = await fetchAllData(
            (endCursor) => createQueryForBookSeries(author, '', endCursor),
            (responseJson) =>
              responseJson?.data?.getCustomerLibrary?.seriesAggregation
                ?.pageInfo,
          );
          console.log('get book series for one author');
          return allFetchedDataForAuthor;
        } catch (e) {
          console.log(`${author}, attempt ${attempt} failed: `, e);
          attempt++;
          if (attempt >= maxAttempts) {
            logErrorDetails(e); // only log when exhausting retries
            return []; // Return an empty array after exhausting retries
          }
          const backoffTime = Math.pow(2, attempt) * 100; // Exponential backoff formula
          await new Promise((resolve) => setTimeout(resolve, backoffTime));
        }
      }
    };
    fetchTasks.push(task);
  }

  const results = await limitConcurrency(fetchTasks, CONCURRENCY_LIMIT);

  console.log(
    `fetchBookSeriesAllAuthors duration: ${(new Date() - fetchBookSeriesAllAuthorsStartTime) / 1000
    } seconds`,
  );
  const flattenedResults = results
    .flat()
    .filter((result) => result.length !== 0);
  const possibleMissingSeriesData = [];
  for (const responseJson of flattenedResults) {
    const seriesCount =
      responseJson?.data?.getCustomerLibrary?.seriesAggregation?.totalCount
        ?.number ?? 0;
    if (seriesCount > 0) {
      possibleMissingSeriesData.push(responseJson);
    }
  }

  // querying book series by genre can be capped by 300 series max across all pages per genre
  // so we find remaining series by searching by author name
  const missingSeriesIds = new Set();
  for (const responseJson of possibleMissingSeriesData) {
    const seriesAggregation =
      responseJson?.data?.getCustomerLibrary?.seriesAggregation;
    // 1 edge = 1 series = 1 node
    const edges = seriesAggregation?.edges ?? [];
    for (const edge of edges) {
      const seriesId = edge?.node?.asin;
      // e.g. Alex Hormozi has Acquisition.com $100M Series and
      // standalone Gym Launch book
      // $100M series alr covered in seriesIds
      if (seriesId && !seriesIds.has(seriesId)) {
        missingSeriesIds.add(seriesId);
      }
    }
  }

  const assignSeriesToBooksRound2StartTime = new Date();
  await assignSeriesToBooks(bookMapByAsin, missingSeriesIds, bookCount);
  console.log(
    `assignSeriesToBooksRound2 duration: ${(new Date() - assignSeriesToBooksRound2StartTime) / 1000
    } seconds`,
  );
}

// get the series each book belong to, if available
// for each book look up series associated with its author
// then assign the right series to each book
// adapted from the 2nd half of addBooksSeries
async function addBooksSeriesTrial(bookMapByAsin) {
  const bookCount = 0;

  // for books without series, search using their author as keywords
  // to see if there's any series by that author
  const authorsForBooksWithoutSeries = new Set();
  for (const book of bookMapByAsin.values()) {
    if (book.authors && book.authors.length > 0) {
      const firstAuthor = book.authors[0];
      authorsForBooksWithoutSeries.add(firstAuthor);
    }
  }
  console.log(
    `getting book series for ${authorsForBooksWithoutSeries.size} authors`,
  );

  const fetchTasks = [];

  const fetchBookSeriesAllAuthorsStartTime = new Date();
  const authorsArray = Array.from(authorsForBooksWithoutSeries);
  // each query for series can only get 300 series max across all pages
  // so further filter each query for series by genre helps get more series overall
  for (const author of authorsArray) {
    const task = async () => {
      const maxAttempts = 3;
      let attempt = 0;
      // todo: remove multiple attempts here, alr built into fetchAllData
      while (attempt < maxAttempts) {
        try {
          const allFetchedDataForAuthor = await fetchAllData(
            (endCursor) => createQueryForBookSeries(author, '', endCursor),
            (responseJson) =>
              responseJson?.data?.getCustomerLibrary?.seriesAggregation
                ?.pageInfo,
          );
          console.log('get book series for one author');
          return allFetchedDataForAuthor;
        } catch (e) {
          console.log(`${author}, attempt ${attempt} failed: `, e);
          attempt++;
          if (attempt >= maxAttempts) {
            logErrorDetails(e); // only log when exhausting retries
            return []; // Return an empty array after exhausting retries
          }
          const backoffTime = Math.pow(2, attempt) * 100; // Exponential backoff formula
          await new Promise((resolve) => setTimeout(resolve, backoffTime));
        }
      }
    };
    fetchTasks.push(task);
  }

  const results = await limitConcurrency(fetchTasks, CONCURRENCY_LIMIT);

  console.log(
    `fetchBookSeriesAllAuthors duration: ${(new Date() - fetchBookSeriesAllAuthorsStartTime) / 1000
    } seconds`,
  );
  const flattenedResults = results
    .flat()
    .filter((result) => result.length !== 0);
  const possibleMissingSeriesData = [];
  for (const responseJson of flattenedResults) {
    const seriesCount =
      responseJson?.data?.getCustomerLibrary?.seriesAggregation?.totalCount
        ?.number ?? 0;
    if (seriesCount > 0) {
      possibleMissingSeriesData.push(responseJson);
    }
  }

  // querying book series by genre can be capped by 300 series max across all pages per genre
  // so we find remaining series by searching by author name
  const missingSeriesIds = new Set();
  for (const responseJson of possibleMissingSeriesData) {
    const seriesAggregation =
      responseJson?.data?.getCustomerLibrary?.seriesAggregation;
    // 1 edge = 1 series = 1 node
    const edges = seriesAggregation?.edges ?? [];
    for (const edge of edges) {
      const seriesId = edge?.node?.asin;
      if (seriesId) {
        missingSeriesIds.add(seriesId);
      }
    }
  }

  const assignSeriesToBooksStartTime = new Date();
  await assignSeriesToBooks(bookMapByAsin, missingSeriesIds, bookCount);
  console.log(
    `assignSeriesToBooks duration: ${(new Date() - assignSeriesToBooksStartTime) / 1000
    } seconds`,
  );
}

async function addBooksGenre(bookMapByAsin) {
  // from looking at Initiator tab (under Network tab) for kindle-reader-api request to server
  // and searching the relevant js file for the uuid corresponding to kindle book type:
  // there are many more uuids, not sure what they mean
  let bookCount = 0;
  const genres = [];
  const getPageInfoFn = function (responseJson) {
    return responseJson?.data?.getCustomerLibrary?.genreAggregation?.pageInfo;
  };
  const allFetchedData = await fetchAllData(
    createQueryForBookGenres,
    getPageInfoFn,
  );
  // get all genres ids and names and subgenres
  for (const responseJson of allFetchedData) {
    const genreAggregation =
      responseJson?.data?.getCustomerLibrary?.genreAggregation;
    // 1 edge = 1 genre = 1 node
    const edges = genreAggregation?.edges ?? [];
    for (const edge of edges) {
      try {
        const node = edge.node;
        /*
                "node": {
                    "id": "172840535725b545acbb01f11a77e366",
                    "name": "Business & Money",
                    "subGenre": [
                      {
                          "id": "3428e14472749b3cead73cd163a4db14",
                          "name": "Management & Leadership",
                      },
                }
                */
        genres.push(node);
      } catch (error) {
        logErrorDetails(error, edge);
      }
    }
  }
  // Create a list of fetch tasks, one for each genre
  const fetchTasks = genres.map((genre) => {
    const genreId = genre.id;
    return fetchAllData(
      (endCursor) => createQueryForBooksInGenre(genreId, endCursor),
      (responseJson) =>
        responseJson?.data?.getCustomerLibrary?.genre?.books?.pageInfo,
    ).catch(error => {
      // catch to avoid Promise.all rejecting at the first failed fetchAllData
      logErrorDetails(error);
      return [];
    });
  });
  // todo: use limitConcurrency?
  // Execute all fetch tasks in parallel and wait for their completion
  let allFetchedResults = await Promise.all(fetchTasks);
  // Flatten the results and filter out empty responses due to errors
  allFetchedResults = allFetchedResults
    .flat()
    .filter((result) => result.length !== 0);
  // get all the books in each genre and add genre field to each book in bookMapByAsin
  for (const responseJson of allFetchedResults) {
    const genreName = responseJson?.data?.getCustomerLibrary?.genre?.name;
    const books = responseJson?.data?.getCustomerLibrary?.genre?.books;
    // 1 edge = 1 book = 1 node
    const edges = books?.edges ?? [];
    for (const edge of edges) {
      try {
        const node = edge.node;
        if (!node || !node.asin) {
          continue;
        }
        const book = bookMapByAsin.get(node.asin);
        // add genreName to a book object
        if (book) {
          if (!book.genres) {
            book.genres = [];
            bookCount += 1;
          }
          book.genres.push(genreName);
        }
      } catch (error) {
        logErrorDetails(error, edge);
      }
    }
    chrome.runtime.sendMessage({
      action: 'updateBookGenreCount',
      count: bookCount,
    });
  }
  // get subgenres separate from genres because subgenres do not "span" the whole genre
  // e.g. 100 books in 1 genre, 3 subgenres, each has 10 books, so 10 books has no subgenre (fetched above)
  const fetchSubgenresTasks = [];
  for (const genre of genres) {
    const genreId = genre.id;
    const genreName = genre.name;
    for (const subGenre of genre.subGenre) {
      const subGenreId = subGenre.id;
      const subGenreName = subGenre.name;
      const task = async () => {
        try {
          const fetchedData = await fetchAllData(
            (endCursor) => createQueryForBooksInGenre(genreId, endCursor, subGenreId),
            (responseJson) =>
              responseJson?.data?.getCustomerLibrary?.genre?.books?.pageInfo,
          );
          return {
            genreName,
            subGenreId,
            subGenreName,
            fetchedData,
          };
        } catch (error) {
          logErrorDetails(error);
          return []; // Return an empty array in case of error to maintain structure
        }
      };
      fetchSubgenresTasks.push(task);
    }
  }
  // Execute all fetch tasks in parallel and wait for their completion
  let allFetchedSubgenresResults = await limitConcurrency(fetchSubgenresTasks, CONCURRENCY_LIMIT);
  // Flatten the results and filter out empty responses due to errors
  allFetchedSubgenresResults = allFetchedSubgenresResults
    .flat()
    .filter((result) => result.fetchedData.length !== 0);
  // get all the books in each subgenre and add subgenre field to each book in bookMapByAsin
  for (const { subGenreName, fetchedData } of allFetchedSubgenresResults) {
    for (const responseJson of fetchedData) {
      const books = responseJson?.data?.getCustomerLibrary?.genre?.books;
      // 1 edge = 1 book = 1 node
      const edges = books?.edges ?? [];
      for (const edge of edges) {
        try {
          const node = edge.node;
          if (!node || !node.asin) {
            continue;
          }
          const book = bookMapByAsin.get(node.asin);
          if (book) {
            if (!book.subGenres) {
              book.subGenres = [];
            }
            book.subGenres.push(subGenreName);
          }
        } catch (error) {
          logErrorDetails(error, edge);
        }
      }
    }
  }
  return genres;
}

async function fetchAllData(createQuery, getPageInfoFn, trialMode = false) {
  const retryDelay = 500; // ms
  const maxAttemptCount = 3;
  let hasNextPage = true;
  let endCursor = '';
  const allFetchedData = [];
  let attemptCount = 0;
  let query = '';
  let responseJson;
  while (hasNextPage) {
    try {
      const csrfTokenMeta = document.querySelector(
        'meta[name="anti-csrftoken-a2z"][id="kindle-reader-api"]',
      );
      const csrfToken = csrfTokenMeta ?
        csrfTokenMeta.getAttribute('content') :
        null;

      if (!csrfToken) {
        logErrorDetails(new Error('csrfToken is null/undefine'));
        throw new Error('CSRF token is null/undefined');
      }

      query = createQuery(endCursor);
      const response = await fetch('https://www.amazon.com/kindle-reader-api', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Anti-Csrftoken-A2z': csrfToken,
        },
        body: JSON.stringify({ query }),
      });

      const responseBody = await response.text(); // Get the response body as text

      const contentType = response.headers.get('Content-Type');
      if (contentType && contentType.includes('text/html')) {
        const parser = new DOMParser();
        const doc = parser.parseFromString(responseBody, "text/html");
        const bodyContent = doc.body ? doc.body.innerHTML : '';
        const error = new Error('Unexpected HTML response');
        error.additionalData = { title: doc.title, body: bodyContent.slice(0, 500) };
        throw error;
      }

      responseJson = JSON.parse(responseBody); // Attempt to parse it as JSON
      if (!response.ok) {
        const error = new Error(`HTTP error! status: ${response.status}, ${response.statusText}`);
        throw error; // Throw the custom error to be caught by an external catch block
      }
      if (responseJson.errors) {
        const errorMessage = `GraphQL Error: ${responseJson.errors.map(error => error.message).join(', ')}`;
        throw Object.assign(new Error(errorMessage), { responseJson });
      }

      chrome.runtime.sendMessage({ action: "apiCallUpdate" });
      allFetchedData.push(responseJson);
      if (trialMode) { // only fetch general info (addBookFormat) for 1 batch
        console.log({ trialMode });
        return allFetchedData;
      }
      const pageInfo = getPageInfoFn(responseJson);
      hasNextPage = pageInfo.hasNextPage ?? false;

      // Pagination Safety Check
      if (hasNextPage && endCursor === pageInfo.endCursor) {
        logErrorDetails(
          new Error(
            `Pagination Warning: endCursor has not changed between requests, potentially causing an infinite loop.`,
          ),
        );
        break; // Exit the loop to prevent an infinite loop
      }
      endCursor = pageInfo.endCursor ?? '';
      // Reset retry counter after successful fetch
      attemptCount = 0;
    } catch (error) {
      logErrorDetails(error, {
        attemptCount: attemptCount,
        query: query,
        response: responseJson,
      });
      const isInvalidSyntaxError =
        responseJson &&
        responseJson.errors &&
        responseJson.errors.some(
          (err) =>
            err.extensions && err.extensions.classification === 'InvalidSyntax',
        );
      if (isInvalidSyntaxError) {
        console.log(
          'Encountered an unretryable error due to invalid syntax. Aborting retries.',
        );
        break; // Exit the retry loop or handle as appropriate
      } else {
        attemptCount++;
        if (attemptCount >= maxAttemptCount) {
          console.log('Max attemptCount reached, aborting fetch');
          // return data collected so far
          return allFetchedData;
        }
        // Exponential backoff for retryable errors
        await new Promise((resolve) =>
          setTimeout(resolve, retryDelay * Math.pow(2, attemptCount)),
        );
      }
    }
  }
  return allFetchedData;
}

// return all books in a map, key is asin
async function getBookMapByAsin(bookMapByAsin) {
  console.log('in getBookMapByAsin');
  let bookCount = 0;

  // from looking at Initiator tab (under Network tab) for kindle-reader-api request to server
  // and searching tee relevant js file for the uuid corresponding to kindle book type:
  // there are many more uuids, not sure what they mean
  const createQueryForGetAllBooks = function (endCursor) {
    return createQuery(300, '', endCursor);
  };
  const getPageInfoFn = function (responseJson) {
    return responseJson?.data?.getCustomerLibrary?.books?.pageInfo;
  };
  const allFetchedData = await fetchAllData(
    createQueryForGetAllBooks,
    getPageInfoFn,
  );
  // get all books from all pages
  // 1 edge = 1 book

  // for loop through allFetchedData
  for (const responseJson of allFetchedData) {
    const edges = responseJson?.data?.getCustomerLibrary?.books?.edges ?? [];
    for (const edge of edges) {
      try {
        const node = edge.node;
        // asin must be present to be key in map
        if (!node || !node.asin) {
          logErrorDetails(new Error('No node or node asin'), node);
          continue;
        }
        const authors = getBookAuthors(node);
        const book = {
          asin: node.asin,
          title: node?.product?.title?.displayString ?? EMPTY_STRING,
          acquiredDate: getReadableAcquiredDate(node),
          // Defensive programming to handle undefined, null
          relationshipSubType: (node.relationshipSubType || []).join(', '),
          relationshipType: node.relationshipType.toLowerCase() ?? EMPTY_STRING,
          authors: authors,
        };

        if (!bookMapByAsin.has(book.asin)) {
          bookMapByAsin.set(book.asin, book);
          bookCount += 1;
        }
        // else { // todo: same asin can have multiple items e.g. sample vs purchase
        //     console.log(`Duplicate book asin: ${book.asin}`);
        // }
      } catch (error) {
        logErrorDetails(error, edge);
      }
    }
    chrome.runtime.sendMessage({
      action: 'updateOverallBookCount',
      count: bookCount,
    });
  }
  return bookMapByAsin;
}

// get data from amazon.com/yourbooks
async function getFullBookList() {
  chrome.runtime.sendMessage({
    action: 'progressDisplay',
  });
  console.log({ MODE });
  console.log('in getFullBookList');
  const bookMapByAsin = new Map();
  if (MODE === 'full access') {
    // get all books, put in maps key being asin
    const getBookMapByAsinStartTime = new Date();
    await getBookMapByAsin(bookMapByAsin);
    console.log(
      `getBookMapByAsin elapsed time: ${(new Date() - getBookMapByAsinStartTime) / 1000
      } seconds`,
    );

    // then get books by format, and add format column to each book
    const addBookFormatStartTime = new Date();
    await addBookFormat(bookMapByAsin);
    console.log(
      `addBookFormat elapsed time: ${(new Date() - addBookFormatStartTime) / 1000
      } seconds`,
    );
  } else {
    // trial mode
    const addBookFormatStartTime = new Date();
    await addBookFormat(bookMapByAsin);
    console.log(
      `addBookFormat elapsed time: ${(new Date() - addBookFormatStartTime) / 1000
      } seconds`,
    );
  }
  // for quick testing:
  // const addMiscDetailsStartTime = new Date();
  // await addMiscDetails(bookMapByAsin);
  // console.log(
  //   `addMiscDetails elapsed time: ${(new Date() - addMiscDetailsStartTime) / 1000
  //   } seconds`,
  // );
  // return;

  /* each genre:
    {
        "id": "172840535725b545acbb01f11a77e366",
        "name": "Business & Money",
    }
    */
  const addBooksGenreStartTime = new Date();
  const genres = await addBooksGenre(bookMapByAsin);
  console.log(
    `addBooksGenre elapsed time: ${(new Date() - addBooksGenreStartTime) / 1000
    } seconds`,
  );

  const addBooksSeriesStartTime = new Date();
  try {
    if (MODE === 'full access') {
      await addBooksSeries(genres, bookMapByAsin);
    } else { // trial
      await addBooksSeriesTrial(bookMapByAsin);
    }
  } catch (error) {
    logErrorDetails(error);
  }
  console.log(
    `addBooksSeries elapsed time: ${(new Date() - addBooksSeriesStartTime) / 1000
    } seconds`,
  );
  const addMiscDetailsStartTime = new Date();
  await addMiscDetails(bookMapByAsin);
  console.log(
    `addMiscDetails elapsed time: ${(new Date() - addMiscDetailsStartTime) / 1000
    } seconds`,
  );
  return bookMapByAsin;
}

// add new column for formattedFirstAuthor of first author
// from full name to <last name, first name>, for sorting later
// take into account titles
// not taken into account weird names like Fustel de Coulanges, Numa Denis
function splitFirstAuthorName(bookMapByAsin) {
  const titles = [
    'MD',
    'M.D.',
    'PhD',
    'Ph.D.',
    'DPhil',
    'D.Phil.',
    'EdD',
    'Ed.D.',
    'D.Ed.',
    'JD',
    'J.D.',
    'MSc',
    'M.Sc.',
    'MS',
    'M.S.',
    'MA',
    'M.A.',
    'MSW',
    'M.S.W.',
    'MBA',
    'M.B.A.',
    'BSc',
    'B.Sc.',
    'BS',
    'B.S.',
    'BA',
    'B.A.',
    'LLB',
    'L.L.B.',
    'LLM',
    'L.L.M.',
    'DVM',
    'D.V.M.',
    'DDS',
    'D.D.S.',
    'OD',
    'O.D.',
    'DO',
    'D.O.',
    'PharmD',
    'Pharm.D.',
    'DNP',
    'D.N.P.',
    'DC',
    'D.C.',
    'DMD',
    'D.M.D.',
    'PsyD',
    'Psy.D.',
    'DrPH',
    'Dr.P.H.',
    'MPH',
    'M.P.H.',
    'RN',
    'R.N.',
    'PA',
    'P.A.',
    'NP',
    'N.P.',
    'RPh',
    'R.Ph.',
    'PT',
    'P.T.',
    'OT',
    'O.T.',
    'Esq',
    'Esq.',
    'L.M.F.T.',
    'LMFT',
    'III',
    'IV',
    'V', // Suffixes for names
    'Sr',
    'Sr.',
    'Jr',
    'Jr.',
    'II', // More suffixes
  ];

  // Function to check if a part is a title
  const isTitle = (part) => titles.includes(part);
  // add a new field 'formattedFirstAuthor' with the rearranged name of first author, for sorting later
  for (const book of bookMapByAsin.values()) {
    try {
      if (book.authors && book.authors.length > 0) {
        const nameParts = book.authors[0].split(' ');
        if (nameParts.length === 1) {
          // If only one part, just use that e.g. Keenan
          book.formattedFirstAuthor = nameParts[0];
          continue;
        }
        const titleIndex = nameParts.findIndex((part) => isTitle(part));

        // If no title is found, assume the last part is the last name
        // lastNameIndex is where last name starts
        const lastNameIndex =
          titleIndex === -1 ? nameParts.length - 1 : titleIndex - 1;

        // Everything before the lastNameIndex is considered the first name
        const firstNameAndMiddle = nameParts
          .slice(0, lastNameIndex)
          .join(' ')
          .trim();
        // Everything from lastNameIndex onwards is the last name, including suffix or title
        const lastName = nameParts.slice(lastNameIndex).join(' ').trim();
        // Add a new field 'formattedFirstAuthor' with the rearranged name
        book.formattedFirstAuthor = `${lastName}, ${firstNameAndMiddle}`;
      } else {
        // Use a default value if no author is specified
        book.formattedFirstAuthor = UNKNOWN;
      }
    } catch (error) {
      logErrorDetails(error, book);
    }
  }
}

// Listen for a message from popup
// popup.js will trigger this section
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'getFullBookList') {
    MODE = request.mode;
    CUSTOMER_NAME = request.customerName;
    CUSTOMER_EMAIL = request.customerEmail;
    CONCURRENCY_LIMIT = request.concurrencyLimit;
    const firstName = getFirstName(CUSTOMER_NAME);
    getFullBookList()
      .then((bookMapByAsin) => {
        let csvData = '';
        // aligns texts to Title column to look good
        csvData += createCsvMessageForUser(6, firstName);
        // column names
        csvData += `"ISBN / ASIN (Amazon ID)","Link","Acquired Date","RelationshipSubtype","RelationshipType","Format","Title","Pages","Listening Length","Genres","Subgenres","Series Position","Series","(First) Author","All Authors"\n`;
        // for each first author, add all their books to the sheet next to each other
        // Assuming bookMapByAsin is a Map where key is asin and value is bookObject

        // sort by the last name of the first author
        let sortedBooks = [];
        splitFirstAuthorName(bookMapByAsin);
        sortedBooks = Array.from(bookMapByAsin.values()).sort((a, b) => {
          let comparisonOutput = 0;
          try {
            // Safely access 'formattedFirstAuthor', defaulting to 'UNKNOWN' if not present
            const authorA = a.formattedFirstAuthor || UNKNOWN;
            const authorB = b.formattedFirstAuthor || UNKNOWN;
            // Since names are already in "last name, first name" format, we can directly compare
            comparisonOutput = authorA.localeCompare(authorB);
            if (comparisonOutput === 0) {
              // If authors are the same, sort by seriesTitle
              const seriesTitleA = a.seriesTitle || '';
              const seriesTitleB = b.seriesTitle || '';
              comparisonOutput = seriesTitleA.localeCompare(seriesTitleB);
              if (comparisonOutput === 0) {
                // If seriesTitle is the same, sort by seriesPosition
                const seriesPositionA = a.seriesPosition || 0;
                const seriesPositionB = b.seriesPosition || 0;
                comparisonOutput = seriesPositionA - seriesPositionB;
                if (comparisonOutput === 0) {
                  // If everything else is the same, sort by title
                  const titleA = a.title || '';
                  const titleB = b.title || '';
                  comparisonOutput = titleA.localeCompare(titleB);
                }
              }
            }
          } catch (error) {
            logErrorDetails(error, { a, b });
          }
          return comparisonOutput;
        });

        for (const book of sortedBooks) {
          try {
            csvData +=
              `="${book.asin}"` +
              `,"${'https://www.amazon.com/dp/' + book.asin}"` +
              `,"${book.acquiredDate}"` +
              `,"${book.relationshipSubType}"` +
              `,"${book.relationshipType}"` +
              `,"${book.format ?? ''}"` +
              `,"${escapeDoubleQuotes(book.title)}"` +
              `,"${book.numberOfPages ?? ''}"` +
              `,"${book.listeningLength ?? ''}"` +
              `,"${book.genres ? book.genres.join(' | ') : ''}"` +
              `,"${book.subGenres ? book.subGenres.join(' | ') : ''}"` +
              `,"${book.seriesPosition ?? ''}"` +
              `,"${book.seriesTitle ?? ''}"` +
              `,"${book.formattedFirstAuthor}"` +
              `,"${book.authors.join(', ')}"\n`; // all authors
          } catch (error) {
            logErrorDetails(error, book);
          }
        }
        // aligns texts to Title column to look good
        csvData += createCsvMessageForUser(6, firstName);
        chrome.runtime.sendMessage({
          action: 'done',
        });

        sendResponse({ csv: csvData });
      })
      .catch((error) => {
        logErrorDetails(error);
      });
    return true; // Enables async sendResponse
  }
});
