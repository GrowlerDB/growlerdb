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

The page is a **self-contained static bundle** (Brand v1.0): `index.html` (inline CSS), `favicon.svg`
(the waterline mark), and `fonts/` (self-hosted Archivo / Instrument Sans / Geist Mono woff2 — no font
CDN). The only external reference is the social image, pulled from `docs.growlerdb.com`. To publish,
sync the bundle (minus this README) to the Apache document root:

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

## Editing

The page follows **Brand v1.0** — see [`../okf/product/brand/`](../okf/product/brand/index.md)
(identity, voice, surfaces). Keep the waterline mark, the dark palette (glacier `#7fa9d4` interactive,
melt `#46b8c8` identity, on `#141517`), the Archivo / Instrument Sans / Geist Mono type, and the copy
in sync with it. Maturity stays **Beta / pre-1.0** — never the "GA / v1.0" the design mocks show (see
the [D40 caveat](../okf/system/decisions/d40-brand-system.md)). Open `index.html` directly in a browser
to preview before deploying.
