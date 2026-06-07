# Changelog

## 0.0.4
  * Exclude Streams that the credentials cannot access (403) from the catalog during discovery; discovery fails only if the credentials cannot read any supported parent stream. [#16](https://github.com/singer-io/tap-notion/pull/16)

## 0.0.3
  * Recursively requests `block_children` data when `has_children` is true [#13](https://github.com/singer-io/tap-notion/pull/13)

## 0.0.2
  * Bump requests to 2.33.0 for security updates [#14](https://github.com/singer-io/tap-notion/pull/14)

## 0.0.1
  * Initial Release
