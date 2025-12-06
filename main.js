import { Actor, log } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';
import { franc } from 'franc';
import { google } from 'googleapis';
import { gotScraping } from 'got-scraping';

const DEFAULT_COUNTRIES = ['ID', 'US', 'MX', 'AR', 'CL', 'CO'];
const COUNTRY_SUBREDDITS = {
    ID: ['indonesia', 'jakarta', 'surabaya', 'bali', 'id'],
    US: ['askanamerican', 'askacademia', 'parenting', 'college', 'unitedstates', 'usa'],
    MX: ['mexico', 'monterrey', 'guadalajara'],
    AR: ['argentina', 'buenosaires', 'devsarg'],
    CL: ['chile', 'santiago'],
    CO: ['colombia', 'bogota', 'medellin'],
};

const MAX_COMMENTS_TO_TAKE = 100;
const UA_POOL = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
];

const pickUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

const normalizeText = (text) => (text ? text.replace(/\s+/g, ' ').trim() : '');

const parseScore = (text) => {
    if (!text) return 0;
    const normalized = text.replace(/[,•]/g, '').toLowerCase();
    const match = normalized.match(/(-?\d+(\.\d+)?)(k)?/);
    if (!match) return 0;
    const value = Number.parseFloat(match[1]);
    return Number.isNaN(value) ? 0 : Math.round(match[3] ? value * 1000 : value);
};

const containsKeyword = (text, keywordMatchers) => {
    if (!text) return null;
    const lowerText = text.toLowerCase();
    for (const matcher of keywordMatchers) {
        if (lowerText.includes(matcher.lower)) return matcher.original;
    }
    return null;
};

const detectLanguage = (text) => {
    if (!text || text.length < 10) return 'und';
    const code3 = franc(text, { minLength: 20 });
    const map = { eng: 'en', ind: 'id', indonesian: 'id', spa: 'es' };
    return map[code3] || code3 || 'und';
};

const inferCountry = (subreddit, lang) => {
    const subLower = subreddit?.toLowerCase() || '';
    for (const [code, list] of Object.entries(COUNTRY_SUBREDDITS)) {
        if (list.includes(subLower)) return code;
    }
    if (lang === 'id') return 'ID';
    if (lang === 'en' && COUNTRY_SUBREDDITS.US.includes(subLower)) return 'US';
    return 'other';
};

const canonicalizeUrl = (url) => {
    try {
        const u = new URL(url);
        return `${u.origin}${u.pathname}`;
    } catch (error) {
        return url;
    }
};

const createSheetsHelpers = async ({ googleServiceAccountKey, spreadsheetId, sheetName }) => {
    if (!googleServiceAccountKey || !spreadsheetId) return null;

    const creds = JSON.parse(googleServiceAccountKey);
    const privateKey = creds.private_key?.replace(/\\n/g, '\n');
    const jwt = new google.auth.JWT(
        creds.client_email,
        null,
        privateKey,
        ['https://www.googleapis.com/auth/spreadsheets'],
    );
    const sheets = google.sheets({ version: 'v4', auth: jwt });

    const header = [
        'country',
        'topic',
        'quote_type',
        'quote',
        'quote_en',
        'post_title',
        'post_title_en',
        'subreddit',
        'score',
        'url',
        'created_at',
        'lang',
        'author',
    ];

    let headerAdded = false;
    const buffer = [];
    const flush = async () => {
        if (!buffer.length) return;
        const chunk = buffer.splice(0, 100);
        const values = headerAdded ? chunk : [header, ...chunk];
        try {
            await sheets.spreadsheets.values.append({
                spreadsheetId,
                range: `${sheetName || 'Sheet1'}!A1`,
                valueInputOption: 'RAW',
                insertDataOption: 'INSERT_ROWS',
                requestBody: { values },
            });
            headerAdded = true;
            log.info(`Sheets: appended ${chunk.length} rows${headerAdded ? '' : ' with header'}.`);
        } catch (error) {
            log.error(`Sheets append failed: ${error.message}`);
        }
    };

    Actor.on('persistState', async () => {
        await flush();
    });
    Actor.on('migrating', async () => {
        await flush();
    });
    Actor.on('aborting', async () => {
        await flush();
    });

    return {
        pushRow: async (row) => {
            buffer.push(row);
            if (buffer.length >= 50) await flush();
        },
        flush,
    };
};

const fetchJsonSearchFallback = async ({ keyword, subreddit, proxyConfiguration, limitParam = 25 }) => {
    const encodedKeyword = encodeURIComponent(keyword);
    const srPart = subreddit ? `/r/${encodeURIComponent(subreddit)}` : '';
    const limit = Math.max(25, Math.min(limitParam, 100));
    const url = `https://www.reddit.com${srPart}/search.json?q=${encodedKeyword}&restrict_sr=${subreddit ? 1 : 0}&sort=new&type=link&t=all&limit=${limit}&raw_json=1&source=recent`;

    try {
        const response = await gotScraping({
            url,
            proxyUrl: await proxyConfiguration?.newUrl(),
            headers: {
                'User-Agent': pickUA(),
                Accept: 'application/json',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                Referer: subreddit
                    ? `https://www.reddit.com/r/${encodeURIComponent(subreddit)}/search/`
                    : 'https://www.reddit.com/search/',
                Origin: 'https://www.reddit.com',
            },
            timeout: { request: 30000 },
        });

        const json = typeof response.body === 'string' ? JSON.parse(response.body) : response.body;
        const children = json?.data?.children || [];
        return children
            .map((c) => c?.data)
            .filter((d) => d && d.permalink && d.title)
            .map((d) => ({
                url: `https://www.reddit.com${d.permalink}`,
                title: d.title,
                score: d.score || 0,
                subreddit: d.subreddit?.toLowerCase() || subreddit || null,
            }));
    } catch (error) {
        log.warning(`JSON search error for ${url}: ${error.message}`);
        return [];
    }
};

const fetchHtmlSearchFallback = async ({ keyword, subreddit, proxyConfiguration }) => {
    const encodedKeyword = encodeURIComponent(keyword);
    const srPart = subreddit ? `/r/${encodeURIComponent(subreddit)}` : '';
    const url = `https://old.reddit.com${srPart}/search/?q=${encodedKeyword}&restrict_sr=${subreddit ? 1 : 0}&sort=new`;

    try {
        const response = await gotScraping({
            url,
            proxyUrl: await proxyConfiguration?.newUrl(),
            headers: {
                'User-Agent': pickUA(),
                Accept: 'text/html',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            },
            timeout: { request: 30000 },
        });

        const body = response.body || '';
        const regex = /<a[^>]+href="([^"]+\/comments\/[^"]+)"[^>]*>(.*?)<\/a>/gi;
        const posts = [];
        const seen = new Set();
        let match;
        while ((match = regex.exec(body)) && posts.length < 50) {
            const href = match[1];
            const title = normalizeText(match[2]);
            if (!href || !title) continue;
            const full = href.startsWith('http') ? href : `https://old.reddit.com${href}`;
            const canonical = canonicalizeUrl(full);
            if (seen.has(canonical)) continue;
            seen.add(canonical);
            const srMatch = canonical.match(/\/r\/([^/]+)/);
            posts.push({
                url: canonical,
                title,
                score: 0,
                subreddit: srMatch ? srMatch[1].toLowerCase() : subreddit || null,
            });
        }
        return posts;
    } catch (error) {
        log.warning(`HTML search error for ${url}: ${error.message}`);
        return [];
    }
};

const fetchPostJson = async ({ url, proxyConfiguration }) => {
    const jsonUrl = url.endsWith('/') ? `${url}.json?raw_json=1` : `${url}/.json?raw_json=1`;
    const response = await gotScraping({
        url: jsonUrl,
        proxyUrl: await proxyConfiguration?.newUrl(),
        headers: {
            'User-Agent': pickUA(),
            Accept: 'application/json',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            Referer: url,
        },
        timeout: { request: 30000 },
    });

    const json = typeof response.body === 'string' ? JSON.parse(response.body) : response.body;
    if (!Array.isArray(json) || json.length < 2) {
        throw new Error('Unexpected post JSON shape');
    }
    return json;
};

const extractCommentsFromJson = (commentData, acc = [], depth = 0) => {
    if (!commentData || typeof commentData !== 'object') return acc;
    const children = commentData.data?.children || [];
    for (const child of children) {
        if (child.kind !== 't1') continue;
        const c = child.data;
        acc.push({
            text: normalizeText(c.body || ''),
            scoreText: String(c.score ?? ''),
            author: normalizeText(c.author || ''),
            createdAt: c.created_utc ? new Date(c.created_utc * 1000).toISOString() : null,
            permalink: c.permalink ? `https://www.reddit.com${c.permalink}` : null,
            id: c.id,
        });
        if (c.replies) extractCommentsFromJson(c.replies, acc, depth + 1);
    }
    return acc;
};

const extractPostFromJson = (json) => {
    try {
        const post = json[0]?.data?.children?.[0]?.data;
        const commentsRaw = json[1];
        if (!post) return null;

        const subreddit = post.subreddit || '';
        const author = post.author || '';
        const createdAt = post.created_utc ? new Date(post.created_utc * 1000).toISOString() : null;
        const score = post.score || 0;
        const title = normalizeText(post.title || '');
        const body = normalizeText(post.selftext || '');
        const comments = extractCommentsFromJson(commentsRaw, [], 0).slice(0, MAX_COMMENTS_TO_TAKE);

        return {
            title,
            body,
            subreddit,
            author,
            createdAt,
            scoreText: String(score),
            comments,
        };
    } catch (error) {
        log.warning(`Failed to parse post JSON: ${error.message}`);
        return null;
    }
};

const extractPostAndCommentsFromPostPage = async (page) => {
    return page.evaluate((maxComments) => {
        const clean = (text) => (text ? text.replace(/\s+/g, ' ').trim() : '');
        const article = document.querySelector('article') || document.querySelector('[data-test-id="post-content"]');
        const title =
            clean(document.querySelector('h1')?.textContent) ||
            clean(article?.querySelector('h1,h2,h3')?.textContent) ||
            '';
        const body =
            clean(article?.querySelector('[data-click-id="text"]')?.innerText) ||
            clean(article?.querySelector('[data-test-id="post-content"]')?.innerText) ||
            '';
        const subredditLink = article?.querySelector('a[href*="/r/"]');
        const subreddit =
            clean(subredditLink?.textContent) ||
            clean(subredditLink?.getAttribute('href')?.match(/\/r\/([^/]+)/)?.[1]) ||
            '';
        const author = clean(article?.querySelector('a[data-click-id="user"]')?.textContent);
        const timeEl = article?.querySelector('time');
        const createdAt = timeEl?.getAttribute('datetime') || null;
        const scoreSource = article?.querySelector('[id^="vote-arrows"]')?.parentElement || article;
        const scoreText = scoreSource?.textContent || '';

        const comments = Array.from(document.querySelectorAll('div[data-testid="comment"]'))
            .slice(0, maxComments)
            .map((comment) => {
                const text = clean(
                    comment.querySelector('[data-testid="comment"]')?.innerText ||
                        comment.querySelector('[data-test-id="comment"]')?.innerText ||
                        comment.innerText,
                );
                const subtitle = comment.querySelector('[data-testid="comment-subtitle"]') || comment;
                const scoreText = subtitle?.textContent || '';
                const author = clean(comment.querySelector('a[data-testid="comment_author_link"]')?.textContent);
                const createdAt = comment.querySelector('time')?.getAttribute('datetime') || null;
                const permalink =
                    comment.querySelector('a[data-testid="comment_permalink_button"]')?.getAttribute('href') ||
                    comment.querySelector('a[href*="/comment/"]')?.getAttribute('href') ||
                    null;
                const id = comment.getAttribute('id');
                return { text, scoreText, author, createdAt, permalink, id };
            })
            .filter((c) => c.text);

        return { title, body, subreddit, author, createdAt, scoreText, comments };
    }, MAX_COMMENTS_TO_TAKE);
};

await Actor.main(async () => {
    const input = (await Actor.getInput()) || {};
    const {
        keywords = [],
        subreddits = [],
        countries = DEFAULT_COUNTRIES,
        maxPostsPerKeyword = 50,
        minScore = 0,
        minTextLength = 50,
        headless = true,
        googleServiceAccountKey,
        spreadsheetId,
        sheetName = 'Sheet1',
        useApifyProxy = true,
        proxyGroups = ['RESIDENTIAL'],
        testMode = false,
    } = input;

    if (!Array.isArray(keywords) || keywords.length === 0) {
        throw new Error('Input "keywords" is required and must be a non-empty array.');
    }

    const keywordMatchers = keywords
        .map((k) => ({ original: k, lower: k.toLowerCase().trim() }))
        .filter((k) => k.lower.length > 0);

    const effectiveKeywords = testMode ? ['clases de programación para niños'] : keywords;
    const effectiveSubreddits = testMode ? ['chile'] : subreddits;

    const requestQueue = await Actor.openRequestQueue();
    const seenPosts = new Set();
    const counters = new Map();

    const proxyConfiguration = await Actor.createProxyConfiguration({
        useApifyProxy,
        groups: proxyGroups && proxyGroups.length ? proxyGroups : undefined,
    });

    const sheetsHelpers = await createSheetsHelpers({ googleServiceAccountKey, spreadsheetId, sheetName });

    for (const keyword of effectiveKeywords) {
        const trimmedKeyword = keyword?.trim();
        if (!trimmedKeyword) continue;

        const subList =
            Array.isArray(effectiveSubreddits) && effectiveSubreddits.length > 0 ? effectiveSubreddits : [null];

        for (const subreddit of subList) {
            const jsonResults = await fetchJsonSearchFallback({
                keyword: trimmedKeyword,
                subreddit,
                proxyConfiguration,
                limitParam: Math.min(100, maxPostsPerKeyword || 50),
            });

            let results = jsonResults;
            if (!results.length) {
                results = await fetchHtmlSearchFallback({
                    keyword: trimmedKeyword,
                    subreddit,
                    proxyConfiguration,
                });
            }

            if (!results.length) {
                log.warning(`No results via JSON/HTML for "${trimmedKeyword}" ${subreddit ? `r/${subreddit}` : ''}`);
                continue;
            }

            const counterKey = `${trimmedKeyword}|${subreddit || 'all'}`;
            for (const post of results) {
                const currentCount = counters.get(counterKey) || 0;
                if (currentCount >= maxPostsPerKeyword) break;

                const canonical = canonicalizeUrl(post.url);
                if (seenPosts.has(canonical)) continue;
                seenPosts.add(canonical);
                counters.set(counterKey, currentCount + 1);

                await requestQueue.addRequest({
                    url: canonical,
                    uniqueKey: canonical,
                    userData: {
                        type: 'post',
                        keyword: trimmedKeyword,
                        subreddit: post.subreddit || subreddit || null,
                        postTitle: post.title,
                    },
                });
            }
            log.info(
                `JSON search got ${results.length} results for "${trimmedKeyword}" ${
                    subreddit ? `r/${subreddit}` : ''
                }`,
            );
        }
    }

    let savedCount = 0;

    const crawler = new PlaywrightCrawler({
        requestQueue,
        maxConcurrency: 1,
        maxRequestRetries: 4,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: { maxPoolSize: 50 },
        proxyConfiguration,
        navigationTimeoutSecs: 90,
        preNavigationHooks: [
            async ({ page, request }) => {
                await page.setExtraHTTPHeaders({
                    'User-Agent': pickUA(),
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                });
                await page.setViewportSize({ width: 1366, height: 768 });
                // Short sleep to reduce burst patterns
                await sleep(500);
                log.info(`Processing ${request.url}`);
            },
        ],
        requestHandlerTimeoutSecs: 180,
        requestHandler: async ({ page, request }) => {
            const { type } = request.userData;
            if (type !== 'post') return;

            const postUrl = request.loadedUrl || request.url;
            const { keyword, subreddit: subFromSearch, postTitle: searchTitle } = request.userData;
            log.info(`Scraping post detail: ${postUrl}`);

            let rawData = null;
            try {
                const json = await fetchPostJson({ url: postUrl, proxyConfiguration });
                rawData = extractPostFromJson(json);
            } catch (error) {
                log.warning(`Post JSON failed for ${postUrl}: ${error.message}`);
            }

            if (!rawData) {
                try {
                    rawData = await extractPostAndCommentsFromPostPage(page);
                } catch (error) {
                    log.warning(`HTML extract failed for ${postUrl}: ${error.message}`);
                    return;
                }
            }

            const postSubreddit = rawData.subreddit || subFromSearch || '';
            const postScore = parseScore(rawData.scoreText);
            const postTitle = normalizeText(rawData.title || searchTitle || '');
            const postBody = normalizeText(rawData.body);
            const postAuthor = normalizeText(rawData.author);
            const postCreatedAt = rawData.createdAt || null;

            const candidates = [];
            if (postTitle) {
                candidates.push({
                    quoteType: 'post_title',
                    text: postTitle,
                    score: postScore,
                    createdAt: postCreatedAt,
                    author: postAuthor,
                    url: postUrl,
                });
            }

            if (postBody) {
                candidates.push({
                    quoteType: 'post_body',
                    text: postBody,
                    score: postScore,
                    createdAt: postCreatedAt,
                    author: postAuthor,
                    url: postUrl,
                });
            }

            for (const comment of rawData.comments || []) {
                const url =
                    comment.permalink && comment.permalink.startsWith('http')
                        ? comment.permalink
                        : comment.permalink
                        ? new URL(comment.permalink, postUrl).href
                        : comment.id
                        ? `${postUrl}#${comment.id}`
                        : postUrl;

                candidates.push({
                    quoteType: 'comment',
                    text: normalizeText(comment.text),
                    score: parseScore(comment.scoreText),
                    createdAt: comment.createdAt || postCreatedAt || null,
                    author: normalizeText(comment.author),
                    url,
                });
            }

            log.info(`Post ${postUrl}: ${candidates.length} candidates (title/body/comments)`);

            for (const candidate of candidates) {
                if (!candidate.text || candidate.text.length < minTextLength) {
                    continue;
                }
                if (candidate.score < minScore) {
                    continue;
                }

                // Используем ключ из поиска, либо ищем вхождение
                const matchedKeyword = containsKeyword(candidate.text, keywordMatchers) || keyword;

                const lang = detectLanguage(candidate.text);
                const country = inferCountry(postSubreddit, lang);

                if (country === 'other' || !countries.includes(country)) {
                    continue;
                }

                const record = {
                    country,
                    topic: matchedKeyword,
                    quote_type: candidate.quoteType,
                    quote: candidate.text,
                    post_title: postTitle,
                    subreddit: postSubreddit,
                    score: candidate.score,
                    url: candidate.url,
                    created_at: candidate.createdAt,
                    lang,
                    author: candidate.author || null,
                };

                await Actor.pushData(record);
                if (sheetsHelpers) {
                    await sheetsHelpers.pushRow([
                        record.country,
                        record.topic,
                        record.quote_type,
                        record.quote,
                        '', // quote_en placeholder
                        record.post_title,
                        '', // post_title_en placeholder
                        record.subreddit,
                        record.score,
                        record.url,
                        record.created_at || '',
                        record.lang,
                        record.author || '',
                    ]);
                }
                savedCount += 1;
                log.info(`Saved record: ${candidate.quoteType} from ${postSubreddit}, country=${country}`);
            }
        },
        failedRequestHandler: async ({ request }) => {
            log.error(`Request failed too many times: ${request.url}`);
        },
    });

    await crawler.run();
    if (sheetsHelpers) {
        await sheetsHelpers.flush();
    }
    log.info(`Run finished. Saved ${savedCount} records to dataset${sheetsHelpers ? ' and attempted Sheets' : ''}.`);
});
