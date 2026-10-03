import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import mdx from "@astrojs/mdx";
import sitemap from "@astrojs/sitemap";

const SITE = "https://pgvis.io";

// https://astro.build/config
export default defineConfig({
  site: SITE,
  integrations: [
    starlight({
      title: "pgvis",
      description:
        "Turn any Postgres database into MCP tools, a PostgREST-compatible REST API, and an OpenAPI 3.0 spec — from one I/O-free Rust engine.",
      favicon: "/app-icon-indigo.svg",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/pgvis/pgvis",
        },
      ],
      sidebar: [
        {
          label: "Getting Started",
          items: [
            { label: "Introduction", slug: "introduction" },
            { label: "Quick Start", slug: "quickstart" },
            { label: "Installation", slug: "installation" },
          ],
        },
        {
          label: "Guides",
          autogenerate: { directory: "guides" },
        },
        {
          label: "Reference",
          autogenerate: { directory: "reference" },
        },
      ],
      customCss: ["./src/styles/custom.css"],
      components: {
        Footer: "./src/components/Footer.astro",
        SiteTitle: "./src/components/SiteTitle.astro",
      },
      head: [
        {
          tag: "meta",
          attrs: { name: "theme-color", content: "#6366f1" },
        },
        {
          tag: "meta",
          attrs: {
            name: "keywords",
            content:
              "pgvis, PostgREST alternative, MCP server, Model Context Protocol, Postgres REST API, OpenAPI 3.0, Rust database API, embeddable database API, LLM database tools",
          },
        },
        { tag: "meta", attrs: { property: "og:image", content: `${SITE}/og-image.png` } },
        { tag: "meta", attrs: { property: "og:image:width", content: "1200" } },
        { tag: "meta", attrs: { property: "og:image:height", content: "630" } },
        { tag: "meta", attrs: { name: "twitter:card", content: "summary_large_image" } },
        { tag: "meta", attrs: { name: "twitter:image", content: `${SITE}/og-image.png` } },
        { tag: "link", attrs: { rel: "preconnect", href: "https://fonts.googleapis.com" } },
        { tag: "link", attrs: { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: true } },
        {
          tag: "link",
          attrs: {
            rel: "stylesheet",
            href: "https://fonts.googleapis.com/css2?family=Geist:wght@700&family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap",
          },
        },
      ],
    }),
    mdx(),
    sitemap({
      changefreq: "weekly",
      priority: 0.7,
    }),
  ],
});
