# growlerdb.com apex landing page

A single self-contained `index.html` (no build step, no external assets) for the **apex**
`growlerdb.com` — the page a reader lands on when they type the bare domain from an announcement.
`docs.growlerdb.com` (the Just-the-Docs site built from [`../docs/`](../docs/)) is unaffected.

> **Why not just serve this from this repo's Pages?** GitHub Pages allows **one custom domain per
> repo**, and this repo's Pages already serves `docs.growlerdb.com` (see [`../docs/CNAME`](../docs/CNAME)).
> So the apex is served separately — either a tiny dedicated Pages repo, or a registrar/Cloudflare
> redirect. Both are below.

## Option A — dedicated GitHub Pages repo (serves this landing page)

1. Create a repo **`GrowlerDB/growlerdb.github.io`** (an org user/organization Pages repo).
2. Copy this folder's contents (`index.html` + `CNAME`) to its root and push to `main`.
3. Repo **Settings → Pages → Source: `main` / root**. Confirm the custom domain shows
   **`growlerdb.com`** and “Enforce HTTPS” once the cert issues.
4. Add the **DNS** records below at the registrar for `growlerdb.com`.

## Option B — pure redirect apex → docs (no landing page)

If you'd rather not maintain a page, redirect the apex straight to the docs site. This still satisfies
"the bare domain resolves." At the DNS/CDN provider (e.g. Cloudflare), add a **redirect rule**
`https://growlerdb.com/* → https://docs.growlerdb.com/` (301) plus the same `www` alias. Registrars
like Namecheap/Porkbun also offer a built-in "URL redirect" record for the apex.

## DNS records (Option A — GitHub Pages apex)

The MX record for `support@growlerdb.com` is untouched by any of this.

| Type | Host | Value |
|---|---|---|
| A | `@` | `185.199.108.153` |
| A | `@` | `185.199.109.153` |
| A | `@` | `185.199.110.153` |
| A | `@` | `185.199.111.153` |
| AAAA | `@` | `2606:50c0:8000::153` |
| AAAA | `@` | `2606:50c0:8001::153` |
| AAAA | `@` | `2606:50c0:8002::153` |
| AAAA | `@` | `2606:50c0:8003::153` |
| CNAME | `www` | `growlerdb.github.io.` |

(The four A / four AAAA addresses are GitHub Pages' published apex IPs — verify against
[GitHub's current list](https://docs.github.com/pages/configuring-a-custom-domain-for-your-github-pages-site/managing-a-custom-domain-for-your-github-pages-site#configuring-an-apex-domain)
before applying, in case they change.)

## Verify (task-277 AC#2 — do this live before announcing)

```sh
dig +short growlerdb.com A
dig +short www.growlerdb.com CNAME
curl -sI https://growlerdb.com/ | head -1        # expect 200 (Option A) or 301 → docs (Option B)
```

Then open **https://growlerdb.com** and **https://www.growlerdb.com** in a browser and confirm the page
(or redirect) loads over HTTPS with a valid certificate.

## Editing

`index.html` is intentionally dependency-free (inline CSS, an inline SVG favicon, the social image
pulled from `docs.growlerdb.com`). Open it directly in a browser to preview. Keep the wordmark, colors
(`#e8590c` accent on `#1e1e1e`), and tagline in sync with [`../docs/img/social-preview.svg`](../docs/img/social-preview.svg)
and the README banner.
