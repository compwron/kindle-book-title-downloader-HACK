
// eslint-disable-next-line no-unused-vars
const EXTENSION_URL = 'https://chromewebstore.google.com/detail/kindle-book-list-exporter/cnmmnejiklbbkapmjegmldhaejjiejbo';

// eslint-disable-next-line no-unused-vars
function getFirstName(fullName) {
  // Define a list of common titles to check against
  const titles = new Set(["mr", "mrs", "ms", "miss", "dr", "prof", "sir"]);

  // Split the full name by spaces and trim any excess whitespace
  const names = fullName.trim().split(/\s+/);

  // Check if the first part is a title, and if so, concatenate it with the next part
  if (names.length > 1 && titles.has(names[0].toLowerCase())) {
    return `${names[0]} ${names[1]}`;
  } else {
    return names[0];
  }
}

// duplicates function in service-worker.js
// because can't import that function to there
// eslint-disable-next-line no-unused-vars
async function getAnalyticsContext() {
  const result = await chrome.storage.local.get(['anonymousId', 'customerEmail']);

  let { anonymousId, customerEmail } = result;
  customerEmail = customerEmail ?? 'unknown';

  if (!anonymousId) {
    anonymousId = Math.random().toString(36).slice(2, 9);
    await chrome.storage.local.set({ 'anonymousId': anonymousId });
  }

  return { anonymousId, customerEmail };
}
