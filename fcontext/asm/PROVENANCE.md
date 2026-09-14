# Vendored Boost.Context fcontext assembly

| Item | Value |
|---|---|
| Upstream | https://github.com/boostorg/context |
| Tag | `boost-1.89.0` |
| Commit | `3af0091efa65892e030add013417840f80b13e08` |
| Paths | `src/asm/{make,jump,ontop}_{x86_64_sysv,arm64_aapcs,arm_aapcs}_elf_gas.S` |
| License | Boost Software License 1.0 (`LICENSE_1_0.txt`) |
| Modifications | None. Files are copied verbatim. |

Symbols are renamed at compile time through the C preprocessor
(`-Djump_fcontext=stackfull_jump_fcontext` and the make/ontop pair, see
`../CMakeLists.txt`), so an application may still link an unmodified
Boost.Context without duplicate-symbol errors.

## SHA-256

```
c7d0e3687cc8d678097befebc0a5072f3db0e87e1f1894b9287d0dbf0c9f248f  jump_arm64_aapcs_elf_gas.S
7a9c50f5178778b4f920c7091235e9ea4fb9329ed73bd25d756bc4f36e335320  jump_arm_aapcs_elf_gas.S
560c7fa5b5c4476999e50e54a6b14efaddb7c7f42a8de8165cd1336c71c93ef0  jump_x86_64_sysv_elf_gas.S
6c4a66435182615097418217af504604ac28f233c4c5289fc2751f05f8aaa78e  make_arm64_aapcs_elf_gas.S
48b89b69b2ef51f49ee078b45a6a508ae0f61297410b5333cf707a1b07b64d1d  make_arm_aapcs_elf_gas.S
31c625da72310d90583e1ef51e2ad57d622ab20fdd9e6d7f2fe60dd5fb6fd7ea  make_x86_64_sysv_elf_gas.S
06683ecffba4fbfbb186bb6e8a6671b91e68e70e3ac75f9ed1dadd59904a4b5d  ontop_arm64_aapcs_elf_gas.S
641a83ab5ff781c0fd1a00e880f94896b08128de761b27a4e6ae149f84168319  ontop_arm_aapcs_elf_gas.S
9f8d3ee83caf715cb2457f1612bfa852efda6966968007efe5e15d5f4517b2bc  ontop_x86_64_sysv_elf_gas.S
c9bff75738922193e67fa726fa225535870d2aa1059f91452c411736284ad566  LICENSE_1_0.txt
```

## Sync

```sh
tag=boost-1.89.0
git clone --depth 1 --branch "$tag" https://github.com/boostorg/context.git /tmp/boost-context
for f in make jump ontop; do
  for a in x86_64_sysv arm64_aapcs arm_aapcs; do
    cp "/tmp/boost-context/src/asm/${f}_${a}_elf_gas.S" .
  done
done
sha256sum *.S LICENSE_1_0.txt   # update the table above
```

## Known upstream gaps to track

- No `bti`/`paciasp` landing pads and no `.note.gnu.property` in the arm64
  files; linking with `-mbranch-protection=standard -z force-bti` will warn and
  drop BTI for the output object.
- x86_64 CET shadow stack support is compiled only when
  `SHADOW_STACK_SYSCALL` is defined; this project leaves it off.
