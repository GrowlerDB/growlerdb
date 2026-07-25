# growlerdb.com apex landing page

A single self-contained `index.html` (no build step, no external assets) for the **apex**
`growlerdb.com` — the page a reader lands on when they type the bare domain from an announcement.
`docs.growlerdb.com` (the Just-the-Docs site built from [`../docs/`](../docs/)) is a separate site and
is unaffected.

## Where it's hosted (live)

Served from a **Google Cloud VM running Apache 2.4**, at **`34.145.3.247`** — apex and `www` both point
there, and Apache redirects `http → https`. TLS is a **Let's Encrypt certificate issued with
`certbot`** (auto-renewing). The MX records for `support@growlerdb.com` (Google) are untouched.

```
apex/www A  ─▶ 34.145.3.247 (GCP VM, Apache)
http://…    ─▶ 301 https://growlerdb.com/
https://…   ─▶ 200, this index.html (Let's Encrypt cert)
```

## DNS records

| Type | Host | Value |
|---|---|---|
| A | `@` | `34.145.3.247` |
| A | `www` | `34.145.3.247` |
| MX | `@` | `smtp.google.com` (unchanged — email is independent) |

## Deploy / update the page

The page is a **self-contained static bundle** (Brand v1.0): `index.html` (inline CSS + inline
[schema.org JSON-LD](#seo--search-engine-submission)), `favicon.svg` (the waterline mark), `fonts/`
(self-hosted Archivo / Instrument Sans / Geist Mono woff2 — no font CDN), plus `robots.txt` and
`sitemap.xml` for crawlers. The only external reference is the social image, pulled from
`docs.growlerdb.com`. To publish, sync the bundle (minus this README) to the Apache document root:

```sh
# from the repo root, on a host with SSH access to the VM
rsync -av --exclude README.md www/ <user>@34.145.3.247:/var/www/growlerdb.com/
# (Apache serves it directly — no reload needed for static files)
```

TLS is managed by certbot on the VM (`certbot renew` runs on a timer); the Apache vhost handles the
`http → https` redirect. Neither needs anything from this repo.

## Verify (do this after any DNS/host/cert change)

```sh
dig +short growlerdb.com A            # → 34.145.3.247
dig +short www.growlerdb.com A        # → 34.145.3.247
curl -sI http://growlerdb.com/  | head -1   # → 301 to https
curl -sI https://growlerdb.com/ | head -1   # → 200 (valid Let's Encrypt cert)
curl -s  -o /dev/null -w '%{ssl_verify_result}\n' https://growlerdb.com/   # → 0 (cert OK)
```

Then open **https://growlerdb.com** and **https://www.growlerdb.com** in a browser and confirm the page
loads over HTTPS with a valid certificate.

## SEO & search-engine submission

The apex page ships the on-page SEO signals directly:

- **`<title>` + meta `description`**, a `canonical` URL, and a `robots` directive
  (`index, follow, max-image-preview:large`).
- **Open Graph + Twitter** card tags (title/description/url + the 1200×630 social image with alt +
  dimensions) so shared links unfurl.
- **[schema.org](https://schema.org) JSON-LD** inline in `index.html` — an `Organization`, a `WebSite`,
  and a free `SoftwareApplication` (`@graph`), so engines model the project (name, description,
  AGPL-3.0 license, GitHub `codeRepository`) for richer results.
- **`robots.txt`** allows all crawlers and advertises `https://growlerdb.com/sitemap.xml`.
- **`sitemap.xml`** lists the apex homepage. The docs are a **separate host**
  (`docs.growlerdb.com`) and publish **their own** sitemap: `jekyll-sitemap` (enabled in
  [`../docs/_config.yml`](../docs/_config.yml)) emits `https://docs.growlerdb.com/sitemap.xml`, and
  [`../docs/robots.txt`](../docs/robots.txt) points at it. Submit **both** sitemaps.

Search engines no longer accept anonymous sitemap "pings" (Google dropped it in 2023; Bing too), so
submission is a one-time, **account-based** step per property. Do it once for **both**
`growlerdb.com` and `docs.growlerdb.com`:

1. **Google Search Console** (<https://search.google.com/search-console>) — add each property,
   verify ownership (easiest is a **DNS `TXT`** record on `growlerdb.com`, which covers both the apex
   and the subdomain via a Domain property), then **Sitemaps → add** `sitemap.xml` for each.
2. **Bing Webmaster Tools** (<https://www.bing.com/webmasters>) — add + verify each site (you can
   **import from Google Search Console** to skip re-verification), then submit each sitemap. Bing feeds
   DuckDuckGo, Ecosia, and Yahoo.
3. **(optional) IndexNow** for instant Bing/Yandex recrawls after an update: generate a key, host it at
   `https://growlerdb.com/<key>.txt`, and `POST` changed URLs to `https://api.indexnow.org/indexnow`.

Verify the crawl surface after any deploy:

```sh
curl -s https://growlerdb.com/robots.txt            # → Allow: / + Sitemap: line
curl -s https://growlerdb.com/sitemap.xml | head -3 # → <urlset> with the apex URL
curl -s https://docs.growlerdb.com/sitemap.xml | head -3   # → jekyll-sitemap output
# Confirm the JSON-LD parses: paste the page into https://validator.schema.org/
```

## Editing

The page follows **Brand v1.0** — see [`../okf/product/brand/`](../okf/product/brand/index.md)
(identity, voice, surfaces). Keep the waterline mark, the dark palette (glacier `#7fa9d4` interactive,
melt `#46b8c8` identity, on `#141517`), the Archivo / Instrument Sans / Geist Mono type, and the copy
in sync with it. Maturity stays **Beta / pre-1.0** — never the "GA / v1.0" the design mocks show (see
the [D40 caveat](../okf/system/decisions/d40-brand-system.md)). Open `index.html` directly in a browser
to preview before deploying.
