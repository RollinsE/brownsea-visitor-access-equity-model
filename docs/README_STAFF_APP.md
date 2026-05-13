# Brownsea staff static app

This folder is a static export of the Brownsea Visitor Access Tool for release `latest`.

It can be published with GitHub Pages, Netlify, or any simple static web host. It does not need Flask, Python, Colab, or a server once exported.

## Main files

- `index.html` - staff postcode lookup app
- `downloads.html` - reports and download links
- `reports/` - released HTML reports and plots
- `artifacts/postcode_shards/` - small postcode-district lookup files used by the app
- `artifacts/` - released CSV/JSON files
- `reports.zip` - downloadable bundle of reports and key audit files

The app loads only the small postcode-district shard needed for the user's search, rather than loading the full postcode lookup file on page open.

Shard files exported: 48

## GitHub Pages

1. Commit this folder as `docs/` in the repository.
2. In GitHub, go to Settings > Pages.
3. Choose `Deploy from a branch`.
4. Select branch `main` and folder `/docs`.
5. Save and use the GitHub Pages URL.

Generated at: 2026-05-11T12:40:51.002504+00:00
