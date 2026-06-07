# Changelog

## 0.0.4
  * Streams the credentials cannot access (403) are now excluded from the catalog during discovery instead of raising an error
  * Child streams are automatically excluded when their parent stream is inaccessible
  * Added unit tests for discovery access checks

## 0.0.3
  * Recursively requests `block_children` data when `has_children` is true [#13](https://github.com/singer-io/tap-notion/pull/13)

## 0.0.2
  * Bump requests to 2.33.0 for security updates [#14](https://github.com/singer-io/tap-notion/pull/14)

## 0.0.1
  * Initial Release
