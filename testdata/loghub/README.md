# loghub-2.0 `2k_dataset` extracts

Vendored from https://github.com/logpai/loghub-2.0 at commit
`ac4aad2ea86f561e5a0acbc6587fa28b487259f9`, directory `2k_dataset/<Set>/`, unmodified.
Three sets are here: `Linux`, `Apache` and `OpenSSH` (issue #5). `Mac` lands with the
harness (issue #13). Each set is the raw log (2,000 lines), the `_structured_corrected.csv`
ground truth (one row per `LineId`, the columns the `extract` node must lift) and
the `_templates_corrected.csv` event templates.

| File | sha256 |
|---|---|
| `Apache/Apache_2k.log` | `0e51c532c9b82b49234f5691ed96d7b584eaeef9f35839b9c365769a80294705` |
| `Apache/Apache_2k.log_structured_corrected.csv` | `cf14ee33db62dd6c9ef2a8c53746a9805ab94fdafb12728067d18d1e706142a5` |
| `Apache/Apache_2k.log_templates_corrected.csv` | `78ee1ad7dc26535454aedbfc8b9e0aae6190b93b321a07276f8037a482998faf` |
| `Linux/Linux_2k.log` | `6d50cefa82380651f910df35fda0995a237a3c788b7b2e3d2d37e51fb9debca9` |
| `Linux/Linux_2k.log_structured_corrected.csv` | `fcfbc63a1f280a45199448642211ee81adab65e7901521fdbb185ebed1ba692c` |
| `Linux/Linux_2k.log_templates_corrected.csv` | `65aaa1c5e6de704e7d25ddf6db1f7425da59246bcb5a6b1c6cdcc5185b347183` |
| `OpenSSH/OpenSSH_2k.log` | `16da02f37eb00cec9ec65c4d71175897be45b266aa7d6e01b26186678e2288b8` |
| `OpenSSH/OpenSSH_2k.log_structured_corrected.csv` | `c2d0f5f538125fed320486fed4f7256ce84ecf0d1174f93cbc2cdb1e5a4c183d` |
| `OpenSSH/OpenSSH_2k.log_templates_corrected.csv` | `3d87ef873dd9ad6f77216d468109a38228cf54881440b68fdbfc4753f16bada0` |

## Licence

This data is **not** under the workspace's Apache-2.0 licence. It is distributed under the
loghub-2.0 licence below, which allows research and academic use and requires the
repository URL and the citations to travel with every copy. Commercial use is not addressed
by that licence. The notice is reproduced verbatim as it requires:

> LICENSE OF LOGHUB-2.0
>
> The datasets are freely available for research or academic work, subject to
> the following condition: For any usage or distribution of the loghub-2.0 datasets,
> please refer to the loghub repository URL (https://github.com/logpai/loghub-2.0)
> and cite the following loghub papers where applicable.
>
> + Zhihan Jiang, Jinyang Liu, Junjie Huang, Yichen Li, Yintong Huo, Jiazhen Gu,
>   Zhuangbin Chen, Jieming Zhu, Michael R. Lyu. A Large-scale Evaluation for
>   Log Parsing Techniques: How Far are We? In ISSTA, 2024.
> + Jieming Zhu, Shilin He, Pinjia He, Jinyang Liu, Michael R. Lyu. Loghub: A Large
>   Collection of System Log Datasets for AI-driven Log Analytics. In ISSRE, 2023.
>
> The above license notice shall be included in all copies of the datasets.

## Comparing extracted attributes with the CSV

The structured CSV was written by pandas and differs from the raw line in three ways. The
extraction tests in `crates/pipeline/tests/extract_loghub.rs` and the harness verifier
(issue #13) apply these rules; the `extract` patterns do not, so the attributes
carry the text as it appears in the line.

1. A cell matching `^\d+\.0$` compares as the integer. `PID` in Linux is `19939.0` for a
   line that reads `[19939]`.
2. An empty cell means the attribute must be absent, the way a capture group that did not
   participate is absent.
3. `Content` is trimmed of trailing whitespace on both sides. Some raw lines end in a
   space that the CSV `Content` cell does not carry. Every other column compares exactly.

Column notes. Linux `Level` is the host name (`combo`), not a severity. OpenSSH
`Component` is the host (`LabSZ`) and the `sshd` token belongs to no column; `Pid` is the
bracketed number. Apache `Time` is the bracketed timestamp without its brackets. `Content`
cells contain commas and are quoted; read the CSV with a real parser.
