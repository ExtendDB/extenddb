# 10 — Dependency Licenses

Generated: 2026-04-23

License approval is checked against `devtools/approved-licenses.txt`.
This file is a human-controlled policy input and must not be modified by agents.

## Summary

| Category | Total | Approved | Not Approved |
|----------|-------|----------|--------------|
| Rust     | 366    | 345       | 21            |
| Python   | 15    | 14       | 1            |
| **Total**| **381**| **359**   | **22**        |

## Not Approved

| Dependency | Version | License | Language |
|------------|---------|---------|----------|
| icu_collections | 2.2.0 | Unicode-3.0 | Rust |
| icu_locale_core | 2.2.0 | Unicode-3.0 | Rust |
| icu_normalizer | 2.2.0 | Unicode-3.0 | Rust |
| icu_normalizer_data | 2.2.0 | Unicode-3.0 | Rust |
| icu_properties | 2.2.0 | Unicode-3.0 | Rust |
| icu_properties_data | 2.2.0 | Unicode-3.0 | Rust |
| icu_provider | 2.2.0 | Unicode-3.0 | Rust |
| litemap | 0.8.2 | Unicode-3.0 | Rust |
| potential_utf | 0.1.5 | Unicode-3.0 | Rust |
| tinystr | 0.8.3 | Unicode-3.0 | Rust |
| unicode-ident | 1.0.24 | (MIT OR Apache-2.0) AND Unicode-3.0 | Rust |
| webpki-roots | 0.26.11 | CDLA-Permissive-2.0 | Rust |
| webpki-roots | 1.0.7 | CDLA-Permissive-2.0 | Rust |
| writeable | 0.6.3 | Unicode-3.0 | Rust |
| yoke | 0.8.2 | Unicode-3.0 | Rust |
| yoke-derive | 0.8.2 | Unicode-3.0 | Rust |
| zerofrom | 0.1.7 | Unicode-3.0 | Rust |
| zerofrom-derive | 0.1.7 | Unicode-3.0 | Rust |
| zerotrie | 0.2.4 | Unicode-3.0 | Rust |
| zerovec | 0.11.6 | Unicode-3.0 | Rust |
| zerovec-derive | 0.11.3 | Unicode-3.0 | Rust |
| certifi | 2026.2.25 | MPL-2.0 | Python |

## Rust Dependencies (366)

| Dependency | Version | License | Approved |
|------------|---------|---------|----------|
| adler2 | 2.0.1 | 0BSD OR MIT OR Apache-2.0 | Yes |
| aead | 0.5.2 | MIT OR Apache-2.0 | Yes |
| aes | 0.8.4 | MIT OR Apache-2.0 | Yes |
| aes-gcm | 0.10.3 | Apache-2.0 OR MIT | Yes |
| ahash | 0.8.12 | MIT OR Apache-2.0 | Yes |
| aho-corasick | 1.1.4 | Unlicense OR MIT | Yes |
| allocator-api2 | 0.2.21 | MIT OR Apache-2.0 | Yes |
| anstream | 1.0.0 | MIT OR Apache-2.0 | Yes |
| anstyle | 1.0.14 | MIT OR Apache-2.0 | Yes |
| anstyle-parse | 1.0.0 | MIT OR Apache-2.0 | Yes |
| anstyle-query | 1.1.5 | MIT OR Apache-2.0 | Yes |
| anstyle-wincon | 3.0.11 | MIT OR Apache-2.0 | Yes |
| anyhow | 1.0.102 | MIT OR Apache-2.0 | Yes |
| arraydeque | 0.5.1 | MIT/Apache-2.0 | Yes |
| async-compression | 0.4.41 | MIT OR Apache-2.0 | Yes |
| async-trait | 0.1.89 | MIT OR Apache-2.0 | Yes |
| atoi | 2.0.0 | MIT | Yes |
| atomic-waker | 1.1.2 | Apache-2.0 OR MIT | Yes |
| autocfg | 1.5.0 | Apache-2.0 OR MIT | Yes |
| aws-lc-rs | 1.16.3 | ISC AND (Apache-2.0 OR ISC) | Yes |
| aws-lc-sys | 0.40.0 | ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0) | Yes |
| axum | 0.8.9 | MIT | Yes |
| axum-core | 0.5.6 | MIT | Yes |
| axum-macros | 0.5.1 | MIT | Yes |
| base64 | 0.21.7 | MIT OR Apache-2.0 | Yes |
| base64 | 0.22.1 | MIT OR Apache-2.0 | Yes |
| base64ct | 1.8.3 | Apache-2.0 OR MIT | Yes |
| bcrypt | 0.17.1 | MIT | Yes |
| bigdecimal | 0.4.10 | MIT/Apache-2.0 | Yes |
| bitflags | 2.11.1 | MIT OR Apache-2.0 | Yes |
| block-buffer | 0.10.4 | MIT OR Apache-2.0 | Yes |
| blowfish | 0.9.1 | MIT OR Apache-2.0 | Yes |
| bumpalo | 3.20.2 | MIT OR Apache-2.0 | Yes |
| byteorder | 1.5.0 | Unlicense OR MIT | Yes |
| bytes | 1.11.1 | MIT | Yes |
| cc | 1.2.60 | MIT OR Apache-2.0 | Yes |
| cfg-if | 1.0.4 | MIT OR Apache-2.0 | Yes |
| cipher | 0.4.4 | MIT OR Apache-2.0 | Yes |
| clap | 4.6.1 | MIT OR Apache-2.0 | Yes |
| clap_builder | 4.6.0 | MIT OR Apache-2.0 | Yes |
| clap_derive | 4.6.1 | MIT OR Apache-2.0 | Yes |
| clap_lex | 1.1.0 | MIT OR Apache-2.0 | Yes |
| cmake | 0.1.58 | MIT OR Apache-2.0 | Yes |
| colorchoice | 1.0.5 | MIT OR Apache-2.0 | Yes |
| compression-codecs | 0.4.37 | MIT OR Apache-2.0 | Yes |
| compression-core | 0.4.31 | MIT OR Apache-2.0 | Yes |
| concurrent-queue | 2.5.0 | Apache-2.0 OR MIT | Yes |
| config | 0.14.1 | MIT OR Apache-2.0 | Yes |
| const-oid | 0.9.6 | Apache-2.0 OR MIT | Yes |
| const-random | 0.1.18 | MIT OR Apache-2.0 | Yes |
| const-random-macro | 0.1.16 | MIT OR Apache-2.0 | Yes |
| convert_case | 0.6.0 | MIT | Yes |
| core-foundation | 0.10.1 | MIT OR Apache-2.0 | Yes |
| core-foundation-sys | 0.8.7 | MIT OR Apache-2.0 | Yes |
| cpufeatures | 0.2.17 | MIT OR Apache-2.0 | Yes |
| crc | 3.4.0 | MIT OR Apache-2.0 | Yes |
| crc-catalog | 2.4.0 | MIT OR Apache-2.0 | Yes |
| crc32fast | 1.5.0 | MIT OR Apache-2.0 | Yes |
| crossbeam-epoch | 0.9.18 | MIT OR Apache-2.0 | Yes |
| crossbeam-queue | 0.3.12 | MIT OR Apache-2.0 | Yes |
| crossbeam-utils | 0.8.21 | MIT OR Apache-2.0 | Yes |
| crunchy | 0.2.4 | MIT | Yes |
| crypto-common | 0.1.7 | MIT OR Apache-2.0 | Yes |
| ctr | 0.9.2 | MIT OR Apache-2.0 | Yes |
| daemonize | 0.5.0 | MIT/Apache-2.0 | Yes |
| der | 0.7.10 | Apache-2.0 OR MIT | Yes |
| deranged | 0.5.8 | MIT OR Apache-2.0 | Yes |
| digest | 0.10.7 | MIT OR Apache-2.0 | Yes |
| displaydoc | 0.2.5 | MIT OR Apache-2.0 | Yes |
| dlv-list | 0.5.2 | MIT OR Apache-2.0 | Yes |
| dotenvy | 0.15.7 | MIT | Yes |
| dunce | 1.0.5 | CC0-1.0 OR MIT-0 OR Apache-2.0 | Yes |
| either | 1.15.0 | MIT OR Apache-2.0 | Yes |
| encoding_rs | 0.8.35 | (Apache-2.0 OR MIT) AND BSD-3-Clause | Yes |
| equivalent | 1.0.2 | Apache-2.0 OR MIT | Yes |
| errno | 0.3.14 | MIT OR Apache-2.0 | Yes |
| etcetera | 0.8.0 | MIT OR Apache-2.0 | Yes |
| event-listener | 5.4.1 | Apache-2.0 OR MIT | Yes |
| find-msvc-tools | 0.1.9 | MIT OR Apache-2.0 | Yes |
| flate2 | 1.1.9 | MIT OR Apache-2.0 | Yes |
| flume | 0.11.1 | Apache-2.0/MIT | Yes |
| fnv | 1.0.7 | Apache-2.0 / MIT | Yes |
| foldhash | 0.1.5 | Zlib | Yes |
| form_urlencoded | 1.2.2 | MIT OR Apache-2.0 | Yes |
| fs_extra | 1.3.0 | MIT | Yes |
| futures-channel | 0.3.32 | MIT OR Apache-2.0 | Yes |
| futures-core | 0.3.32 | MIT OR Apache-2.0 | Yes |
| futures-executor | 0.3.32 | MIT OR Apache-2.0 | Yes |
| futures-intrusive | 0.5.0 | MIT OR Apache-2.0 | Yes |
| futures-io | 0.3.32 | MIT OR Apache-2.0 | Yes |
| futures-sink | 0.3.32 | MIT OR Apache-2.0 | Yes |
| futures-task | 0.3.32 | MIT OR Apache-2.0 | Yes |
| futures-util | 0.3.32 | MIT OR Apache-2.0 | Yes |
| generic-array | 0.14.7 | MIT | Yes |
| getrandom | 0.2.17 | MIT OR Apache-2.0 | Yes |
| getrandom | 0.3.4 | MIT OR Apache-2.0 | Yes |
| getrandom | 0.4.2 | MIT OR Apache-2.0 | Yes |
| ghash | 0.5.1 | Apache-2.0 OR MIT | Yes |
| h2 | 0.4.13 | MIT | Yes |
| hashbrown | 0.14.5 | MIT OR Apache-2.0 | Yes |
| hashbrown | 0.15.5 | MIT OR Apache-2.0 | Yes |
| hashbrown | 0.17.0 | MIT OR Apache-2.0 | Yes |
| hashlink | 0.10.0 | MIT OR Apache-2.0 | Yes |
| hashlink | 0.8.4 | MIT OR Apache-2.0 | Yes |
| heck | 0.5.0 | MIT OR Apache-2.0 | Yes |
| hex | 0.4.3 | MIT OR Apache-2.0 | Yes |
| hkdf | 0.12.4 | MIT OR Apache-2.0 | Yes |
| hmac | 0.12.1 | MIT OR Apache-2.0 | Yes |
| home | 0.5.12 | MIT OR Apache-2.0 | Yes |
| http | 1.4.0 | MIT OR Apache-2.0 | Yes |
| http-body | 1.0.1 | MIT | Yes |
| http-body-util | 0.1.3 | MIT | Yes |
| httparse | 1.10.1 | MIT OR Apache-2.0 | Yes |
| httpdate | 1.0.3 | MIT OR Apache-2.0 | Yes |
| hyper | 1.9.0 | MIT | Yes |
| hyper-rustls | 0.27.9 | Apache-2.0 OR ISC OR MIT | Yes |
| hyper-util | 0.1.20 | MIT | Yes |
| icu_collections | 2.2.0 | Unicode-3.0 | **No** |
| icu_locale_core | 2.2.0 | Unicode-3.0 | **No** |
| icu_normalizer | 2.2.0 | Unicode-3.0 | **No** |
| icu_normalizer_data | 2.2.0 | Unicode-3.0 | **No** |
| icu_properties | 2.2.0 | Unicode-3.0 | **No** |
| icu_properties_data | 2.2.0 | Unicode-3.0 | **No** |
| icu_provider | 2.2.0 | Unicode-3.0 | **No** |
| id-arena | 2.3.0 | MIT/Apache-2.0 | Yes |
| idna | 1.1.0 | MIT OR Apache-2.0 | Yes |
| idna_adapter | 1.2.1 | Apache-2.0 OR MIT | Yes |
| indexmap | 2.14.0 | Apache-2.0 OR MIT | Yes |
| inout | 0.1.4 | MIT OR Apache-2.0 | Yes |
| ipnet | 2.12.0 | MIT OR Apache-2.0 | Yes |
| is_terminal_polyfill | 1.70.2 | MIT OR Apache-2.0 | Yes |
| itoa | 1.0.18 | MIT OR Apache-2.0 | Yes |
| jobserver | 0.1.34 | MIT OR Apache-2.0 | Yes |
| js-sys | 0.3.95 | MIT OR Apache-2.0 | Yes |
| json5 | 0.4.1 | ISC | Yes |
| lazy_static | 1.5.0 | MIT OR Apache-2.0 | Yes |
| leb128fmt | 0.1.0 | MIT OR Apache-2.0 | Yes |
| libc | 0.2.185 | MIT OR Apache-2.0 | Yes |
| libm | 0.2.16 | MIT | Yes |
| libredox | 0.1.16 | MIT | Yes |
| libsqlite3-sys | 0.30.1 | MIT | Yes |
| litemap | 0.8.2 | Unicode-3.0 | **No** |
| lock_api | 0.4.14 | MIT OR Apache-2.0 | Yes |
| log | 0.4.29 | MIT OR Apache-2.0 | Yes |
| matchers | 0.2.0 | MIT | Yes |
| matchit | 0.8.4 | MIT AND BSD-3-Clause | Yes |
| md-5 | 0.10.6 | MIT OR Apache-2.0 | Yes |
| memchr | 2.8.0 | Unlicense OR MIT | Yes |
| metrics | 0.24.3 | MIT | Yes |
| metrics-util | 0.19.1 | MIT | Yes |
| mime | 0.3.17 | MIT OR Apache-2.0 | Yes |
| minimal-lexical | 0.2.1 | MIT/Apache-2.0 | Yes |
| miniz_oxide | 0.8.9 | MIT OR Zlib OR Apache-2.0 | Yes |
| mio | 1.2.0 | MIT | Yes |
| nom | 7.1.3 | MIT | Yes |
| nu-ansi-term | 0.50.3 | MIT | Yes |
| num-bigint | 0.4.6 | MIT OR Apache-2.0 | Yes |
| num-bigint-dig | 0.8.6 | MIT/Apache-2.0 | Yes |
| num-conv | 0.2.1 | MIT OR Apache-2.0 | Yes |
| num-integer | 0.1.46 | MIT OR Apache-2.0 | Yes |
| num-iter | 0.1.45 | MIT OR Apache-2.0 | Yes |
| num-traits | 0.2.19 | MIT OR Apache-2.0 | Yes |
| once_cell | 1.21.4 | MIT OR Apache-2.0 | Yes |
| once_cell_polyfill | 1.70.2 | MIT OR Apache-2.0 | Yes |
| opaque-debug | 0.3.1 | MIT OR Apache-2.0 | Yes |
| openssl-probe | 0.2.1 | MIT OR Apache-2.0 | Yes |
| ordered-multimap | 0.7.3 | MIT | Yes |
| parking | 2.2.1 | Apache-2.0 OR MIT | Yes |
| parking_lot | 0.12.5 | MIT OR Apache-2.0 | Yes |
| parking_lot_core | 0.9.12 | MIT OR Apache-2.0 | Yes |
| pathdiff | 0.2.3 | MIT/Apache-2.0 | Yes |
| pem-rfc7468 | 0.7.0 | Apache-2.0 OR MIT | Yes |
| percent-encoding | 2.3.2 | MIT OR Apache-2.0 | Yes |
| pest | 2.8.6 | MIT OR Apache-2.0 | Yes |
| pest_derive | 2.8.6 | MIT OR Apache-2.0 | Yes |
| pest_generator | 2.8.6 | MIT OR Apache-2.0 | Yes |
| pest_meta | 2.8.6 | MIT OR Apache-2.0 | Yes |
| pin-project-lite | 0.2.17 | Apache-2.0 OR MIT | Yes |
| pkcs1 | 0.7.5 | Apache-2.0 OR MIT | Yes |
| pkcs8 | 0.10.2 | Apache-2.0 OR MIT | Yes |
| pkg-config | 0.3.33 | MIT OR Apache-2.0 | Yes |
| plain | 0.2.3 | MIT/Apache-2.0 | Yes |
| polyval | 0.6.2 | Apache-2.0 OR MIT | Yes |
| portable-atomic | 1.13.1 | Apache-2.0 OR MIT | Yes |
| potential_utf | 0.1.5 | Unicode-3.0 | **No** |
| powerfmt | 0.2.0 | MIT OR Apache-2.0 | Yes |
| ppv-lite86 | 0.2.21 | MIT OR Apache-2.0 | Yes |
| prettyplease | 0.2.37 | MIT OR Apache-2.0 | Yes |
| proc-macro2 | 1.0.106 | MIT OR Apache-2.0 | Yes |
| quanta | 0.12.6 | MIT | Yes |
| quote | 1.0.45 | MIT OR Apache-2.0 | Yes |
| r-efi | 5.3.0 | MIT OR Apache-2.0 OR LGPL-2.1-or-later | Yes |
| r-efi | 6.0.0 | MIT OR Apache-2.0 OR LGPL-2.1-or-later | Yes |
| rand | 0.8.6 | MIT OR Apache-2.0 | Yes |
| rand | 0.9.4 | MIT OR Apache-2.0 | Yes |
| rand_chacha | 0.3.1 | MIT OR Apache-2.0 | Yes |
| rand_chacha | 0.9.0 | MIT OR Apache-2.0 | Yes |
| rand_core | 0.6.4 | MIT OR Apache-2.0 | Yes |
| rand_core | 0.9.5 | MIT OR Apache-2.0 | Yes |
| rand_xoshiro | 0.7.0 | MIT OR Apache-2.0 | Yes |
| raw-cpuid | 11.6.0 | MIT | Yes |
| redox_syscall | 0.5.18 | MIT | Yes |
| redox_syscall | 0.7.4 | MIT | Yes |
| regex-automata | 0.4.14 | MIT OR Apache-2.0 | Yes |
| regex-syntax | 0.8.10 | MIT OR Apache-2.0 | Yes |
| ring | 0.17.14 | Apache-2.0 AND ISC | Yes |
| ron | 0.8.1 | MIT OR Apache-2.0 | Yes |
| rsa | 0.9.10 | MIT OR Apache-2.0 | Yes |
| rust-ini | 0.20.0 | MIT | Yes |
| rustls | 0.23.39 | Apache-2.0 OR ISC OR MIT | Yes |
| rustls-native-certs | 0.8.3 | Apache-2.0 OR ISC OR MIT | Yes |
| rustls-pki-types | 1.14.0 | MIT OR Apache-2.0 | Yes |
| rustls-webpki | 0.103.13 | ISC | Yes |
| rustversion | 1.0.22 | MIT OR Apache-2.0 | Yes |
| ryu | 1.0.23 | Apache-2.0 OR BSL-1.0 | Yes |
| schannel | 0.1.29 | MIT | Yes |
| scopeguard | 1.2.0 | MIT OR Apache-2.0 | Yes |
| security-framework | 3.7.0 | MIT OR Apache-2.0 | Yes |
| security-framework-sys | 2.17.0 | MIT OR Apache-2.0 | Yes |
| semver | 1.0.28 | MIT OR Apache-2.0 | Yes |
| serde | 1.0.228 | MIT OR Apache-2.0 | Yes |
| serde_core | 1.0.228 | MIT OR Apache-2.0 | Yes |
| serde_derive | 1.0.228 | MIT OR Apache-2.0 | Yes |
| serde_json | 1.0.149 | MIT OR Apache-2.0 | Yes |
| serde_path_to_error | 0.1.20 | MIT OR Apache-2.0 | Yes |
| serde_spanned | 0.6.9 | MIT OR Apache-2.0 | Yes |
| serde_urlencoded | 0.7.1 | MIT/Apache-2.0 | Yes |
| sha1 | 0.10.6 | MIT OR Apache-2.0 | Yes |
| sha2 | 0.10.9 | MIT OR Apache-2.0 | Yes |
| sharded-slab | 0.1.7 | MIT | Yes |
| shlex | 1.3.0 | MIT OR Apache-2.0 | Yes |
| signal-hook-registry | 1.4.8 | MIT OR Apache-2.0 | Yes |
| signature | 2.2.0 | Apache-2.0 OR MIT | Yes |
| simd-adler32 | 0.3.9 | MIT | Yes |
| sketches-ddsketch | 0.3.1 | Apache-2.0 | Yes |
| slab | 0.4.12 | MIT | Yes |
| smallvec | 1.15.1 | MIT OR Apache-2.0 | Yes |
| socket2 | 0.6.3 | MIT OR Apache-2.0 | Yes |
| spin | 0.9.8 | MIT | Yes |
| spki | 0.7.3 | Apache-2.0 OR MIT | Yes |
| sqlx | 0.8.6 | MIT OR Apache-2.0 | Yes |
| sqlx-core | 0.8.6 | MIT OR Apache-2.0 | Yes |
| sqlx-macros | 0.8.6 | MIT OR Apache-2.0 | Yes |
| sqlx-macros-core | 0.8.6 | MIT OR Apache-2.0 | Yes |
| sqlx-mysql | 0.8.6 | MIT OR Apache-2.0 | Yes |
| sqlx-postgres | 0.8.6 | MIT OR Apache-2.0 | Yes |
| sqlx-sqlite | 0.8.6 | MIT OR Apache-2.0 | Yes |
| stable_deref_trait | 1.2.1 | MIT OR Apache-2.0 | Yes |
| stringprep | 0.1.5 | MIT/Apache-2.0 | Yes |
| strsim | 0.11.1 | MIT | Yes |
| subtle | 2.6.1 | BSD-3-Clause | Yes |
| syn | 2.0.117 | MIT OR Apache-2.0 | Yes |
| sync_wrapper | 1.0.2 | Apache-2.0 | Yes |
| synstructure | 0.13.2 | MIT | Yes |
| syslog-tracing | 0.3.1 | MIT | Yes |
| thiserror | 1.0.69 | MIT OR Apache-2.0 | Yes |
| thiserror | 2.0.18 | MIT OR Apache-2.0 | Yes |
| thiserror-impl | 1.0.69 | MIT OR Apache-2.0 | Yes |
| thiserror-impl | 2.0.18 | MIT OR Apache-2.0 | Yes |
| thread_local | 1.1.9 | MIT OR Apache-2.0 | Yes |
| time | 0.3.47 | MIT OR Apache-2.0 | Yes |
| time-core | 0.1.8 | MIT OR Apache-2.0 | Yes |
| time-macros | 0.2.27 | MIT OR Apache-2.0 | Yes |
| tiny-keccak | 2.0.2 | CC0-1.0 | Yes |
| tinystr | 0.8.3 | Unicode-3.0 | **No** |
| tinyvec | 1.11.0 | Zlib OR Apache-2.0 OR MIT | Yes |
| tinyvec_macros | 0.1.1 | MIT OR Apache-2.0 OR Zlib | Yes |
| tokio | 1.52.1 | MIT | Yes |
| tokio-macros | 2.7.0 | MIT | Yes |
| tokio-rustls | 0.26.4 | MIT OR Apache-2.0 | Yes |
| tokio-stream | 0.1.18 | MIT | Yes |
| tokio-util | 0.7.18 | MIT | Yes |
| toml | 0.8.23 | MIT OR Apache-2.0 | Yes |
| toml_datetime | 0.6.11 | MIT OR Apache-2.0 | Yes |
| toml_edit | 0.22.27 | MIT OR Apache-2.0 | Yes |
| toml_write | 0.1.2 | MIT OR Apache-2.0 | Yes |
| tower | 0.5.3 | MIT | Yes |
| tower-http | 0.6.8 | MIT | Yes |
| tower-layer | 0.3.3 | MIT | Yes |
| tower-service | 0.3.3 | MIT | Yes |
| tracing | 0.1.44 | MIT | Yes |
| tracing-attributes | 0.1.31 | MIT | Yes |
| tracing-core | 0.1.36 | MIT | Yes |
| tracing-log | 0.2.0 | MIT | Yes |
| tracing-serde | 0.2.0 | MIT | Yes |
| tracing-subscriber | 0.3.23 | MIT | Yes |
| try-lock | 0.2.5 | MIT | Yes |
| typenum | 1.20.0 | MIT OR Apache-2.0 | Yes |
| ucd-trie | 0.1.7 | MIT OR Apache-2.0 | Yes |
| unicode-bidi | 0.3.18 | MIT OR Apache-2.0 | Yes |
| unicode-ident | 1.0.24 | (MIT OR Apache-2.0) AND Unicode-3.0 | **No** |
| unicode-normalization | 0.1.25 | MIT OR Apache-2.0 | Yes |
| unicode-properties | 0.1.4 | MIT/Apache-2.0 | Yes |
| unicode-segmentation | 1.13.2 | MIT OR Apache-2.0 | Yes |
| unicode-xid | 0.2.6 | MIT OR Apache-2.0 | Yes |
| universal-hash | 0.5.1 | MIT OR Apache-2.0 | Yes |
| untrusted | 0.9.0 | ISC | Yes |
| url | 2.5.8 | MIT OR Apache-2.0 | Yes |
| utf8_iter | 1.0.4 | Apache-2.0 OR MIT | Yes |
| utf8parse | 0.2.2 | Apache-2.0 OR MIT | Yes |
| uuid | 1.23.1 | Apache-2.0 OR MIT | Yes |
| valuable | 0.1.1 | MIT | Yes |
| vcpkg | 0.2.15 | MIT/Apache-2.0 | Yes |
| version_check | 0.9.5 | MIT/Apache-2.0 | Yes |
| want | 0.3.1 | MIT | Yes |
| wasi | 0.11.1+wasi-snapshot-preview1 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wasip2 | 1.0.3+wasi-0.2.9 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wasip3 | 0.4.0+wasi-0.3.0-rc-2026-01-06 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wasite | 0.1.0 | Apache-2.0 OR BSL-1.0 OR MIT | Yes |
| wasm-bindgen | 0.2.118 | MIT OR Apache-2.0 | Yes |
| wasm-bindgen-macro | 0.2.118 | MIT OR Apache-2.0 | Yes |
| wasm-bindgen-macro-support | 0.2.118 | MIT OR Apache-2.0 | Yes |
| wasm-bindgen-shared | 0.2.118 | MIT OR Apache-2.0 | Yes |
| wasm-encoder | 0.244.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wasm-metadata | 0.244.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wasmparser | 0.244.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| web-sys | 0.3.95 | MIT OR Apache-2.0 | Yes |
| webpki-roots | 0.26.11 | CDLA-Permissive-2.0 | **No** |
| webpki-roots | 1.0.7 | CDLA-Permissive-2.0 | **No** |
| whoami | 1.6.1 | Apache-2.0 OR BSL-1.0 OR MIT | Yes |
| winapi | 0.3.9 | MIT/Apache-2.0 | Yes |
| winapi-i686-pc-windows-gnu | 0.4.0 | MIT/Apache-2.0 | Yes |
| winapi-x86_64-pc-windows-gnu | 0.4.0 | MIT/Apache-2.0 | Yes |
| windows-link | 0.2.1 | MIT OR Apache-2.0 | Yes |
| windows-sys | 0.48.0 | MIT OR Apache-2.0 | Yes |
| windows-sys | 0.52.0 | MIT OR Apache-2.0 | Yes |
| windows-sys | 0.61.2 | MIT OR Apache-2.0 | Yes |
| windows-targets | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows-targets | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_aarch64_gnullvm | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_aarch64_gnullvm | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_aarch64_msvc | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_aarch64_msvc | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_i686_gnu | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_i686_gnu | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_i686_gnullvm | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_i686_msvc | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_i686_msvc | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_x86_64_gnu | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_x86_64_gnu | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_x86_64_gnullvm | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_x86_64_gnullvm | 0.52.6 | MIT OR Apache-2.0 | Yes |
| windows_x86_64_msvc | 0.48.5 | MIT OR Apache-2.0 | Yes |
| windows_x86_64_msvc | 0.52.6 | MIT OR Apache-2.0 | Yes |
| winnow | 0.7.15 | MIT | Yes |
| wit-bindgen | 0.51.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wit-bindgen | 0.57.1 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wit-bindgen-core | 0.51.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wit-bindgen-rust | 0.51.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wit-bindgen-rust-macro | 0.51.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wit-component | 0.244.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| wit-parser | 0.244.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | Yes |
| writeable | 0.6.3 | Unicode-3.0 | **No** |
| yaml-rust2 | 0.8.1 | MIT OR Apache-2.0 | Yes |
| yoke | 0.8.2 | Unicode-3.0 | **No** |
| yoke-derive | 0.8.2 | Unicode-3.0 | **No** |
| zerocopy | 0.8.48 | BSD-2-Clause OR Apache-2.0 OR MIT | Yes |
| zerocopy-derive | 0.8.48 | BSD-2-Clause OR Apache-2.0 OR MIT | Yes |
| zerofrom | 0.1.7 | Unicode-3.0 | **No** |
| zerofrom-derive | 0.1.7 | Unicode-3.0 | **No** |
| zeroize | 1.8.2 | Apache-2.0 OR MIT | Yes |
| zerotrie | 0.2.4 | Unicode-3.0 | **No** |
| zerovec | 0.11.6 | Unicode-3.0 | **No** |
| zerovec-derive | 0.11.3 | Unicode-3.0 | **No** |
| zmij | 1.0.21 | MIT | Yes |

## Python Dependencies (15)

| Dependency | Version | License | Approved |
|------------|---------|---------|----------|
| boto3 | 1.33.13 | Apache-2.0 | Yes |
| botocore | 1.33.13 | Apache-2.0 | Yes |
| certifi | 2026.2.25 | MPL-2.0 | **No** |
| charset-normalizer | 3.4.7 | MIT | Yes |
| idna | 3.10 | BSD-3-Clause | Yes |
| jmespath | 1.0.1 | MIT | Yes |
| Markdown | 3.4.4 | BSD-3-Clause | Yes |
| pytest | 7.4.4 | MIT | Yes |
| pytest-cov | 4.1.0 | MIT | Yes |
| python-dateutil | 2.9.0.post0 | Apache-2.0 OR BSD-3-Clause | Yes |
| requests | 2.31.0 | Apache-2.0 | Yes |
| s3transfer | 0.8.2 | Apache-2.0 | Yes |
| six | 1.17.0 | MIT | Yes |
| urllib3 | 1.26.20 | MIT | Yes |
| WeasyPrint | 52.5 | BSD-3-Clause | Yes |

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
