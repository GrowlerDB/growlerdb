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

`index.html` is dependency-free (inline CSS, an inline SVG favicon, the social image pulled from
`docs.growlerdb.com`). To publish an edit, copy it to the Apache document root on the VM and reload:

```sh
# from the repo root, on a host with SSH access to the VM
scp www/index.html <user>@34.145.3.247:/var/www/growlerdb.com/index.html
# (Apache serves it directly — no reload needed for a static file)
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

Keep the wordmark, colors (`#e8590c` accent on `#1e1e1e`), and tagline in sync with
[`../docs/img/social-preview.svg`](../docs/img/social-preview.svg) and the README banner. Open
`index.html` directly in a browser to preview before deploying.
