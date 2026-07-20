# Third-party licenses

This file covers two things, so a binary release archive carries the attribution
for everything linked into it:

1. **Adapted source code** vendored into this repository (the `retry` package),
   with its provenance and what changed from upstream — the section immediately
   below.
2. **Binary dependencies** — every Go module compiled into the `tinvest`
   executable (direct and transitive), with each module's license text — the
   "Binary dependencies" section further down.

The full license texts referenced by both sections (Apache 2.0, 3-clause BSD,
MIT, and the Go patent grant) are reproduced at the end of this file.

## internal/transport/retry/

**Files:** `internal/transport/retry/retry.go`, `internal/transport/retry/backoff.go`

**Adapted from:** [`github.com/russianinvestments/invest-api-go-sdk`](https://github.com/RussianInvestments/invest-api-go-sdk),
tag `v1.40.1`, commit `9c992f69a3a6ba3ed308e04e3b685eb6c550a109`, package `retry/`
(`retry.go`, `backoff.go`). Apache License 2.0.

That package is itself, per its own file headers, a vendored copy of
[`go-grpc-middleware/v2`](https://github.com/grpc-ecosystem/go-grpc-middleware)'s
gRPC retry interceptor (`interceptors/retry/`):

```
Copyright (c) The go-grpc-middleware Authors.
Licensed under the Apache License 2.0.
```

**What changed from upstream** (see the header comment in each file for
detail):

- Upstream exposes two separate interceptors with independent retry-attempt
  budgets: `UnaryClientInterceptor` (generic gRPC-code retry) and
  `UnaryClientInterceptorRE` (RESOURCE_EXHAUSTED-only, honoring the
  `x-ratelimit-reset` trailer). This port merges both into a single loop
  driven by one `RetryPolicy` and one shared `MaxAttempts` budget.
- Upstream's exponential backoff (`BackoffExponential`) has neither a cap nor
  jitter — flagged in `docs/sdk-spike.md` §1 as a thundering-herd risk,
  particularly on the RESOURCE_EXHAUSTED path where many local processes can
  hit the same per-token limit simultaneously. This port adds both a cap and
  jitter (`exponentialJitterBackoff`, `jitterUp` in `backoff.go`).
- Upstream retries indiscriminately by gRPC code, with no notion of
  idempotency. This port adds idempotency-aware gating (`idempotent.go`,
  `methods.go`, not derived from upstream): a call is only retried if it is a
  recognized read RPC (an explicit `Get*`/`Find*` prefix allowlist plus a
  small list of known read RPCs that don't follow that naming convention) or
  the caller explicitly marked its context with `retry.Idempotent`.
- Upstream's stream-retry machinery (`retryingStream`, buffered-send replay)
  was not ported — out of scope for this package; stream reconnect is
  planned as its own component (`internal/streamer`, see
  `docs/plan-tinvest-cli.md` §9/roadmap M3) with its own resubscription and
  snapshot-reconciliation design, not a port of upstream's buffered-replay
  approach (`docs/sdk-spike.md` §2).
- Upstream's `CallOption`-based per-call override system (`options.go`), its
  `AttemptMetadataKey` header injection, and its `golang.org/x/net/trace`
  logging integration were not ported; this package has no dependency on
  `go-grpc-middleware` or any upstream SDK/config/logger types — only
  `google.golang.org/grpc`, `codes`, `status`, and `metadata`.
- Copyright headers from the upstream files are preserved verbatim (plus an
  adaptation note) on the two files containing derived logic
  (`retry.go`, `backoff.go`). Files with no upstream-derived logic
  (`idempotent.go`, `methods.go`, and this package's `RetryPolicy` type,
  wiring into `internal/transport`) carry no such header, since they are
  original to this repository.

---

## Binary dependencies

Every Go module linked into the `tinvest` binary, from `go version -m` on a
release build (direct and transitive). The `mousetrap` module is compiled only
into Windows builds; it is listed because the Windows release archives contain
it. Contract stubs under `pb/investapi` are generated from the vendored protos in
`proto/` (see `proto/VERSION.md`), not a linked module.

| Module | Version | License |
|---|---|---|
| github.com/BurntSushi/toml | v1.6.0 | MIT |
| github.com/spf13/cobra | v1.10.2 | Apache-2.0 |
| github.com/spf13/pflag | v1.0.9 | BSD-3-Clause |
| github.com/inconshreveable/mousetrap (Windows only) | v1.1.0 | Apache-2.0 |
| golang.org/x/net | v0.53.0 | BSD-3-Clause (+ Go patent grant) |
| golang.org/x/sys | v0.47.0 | BSD-3-Clause (+ Go patent grant) |
| golang.org/x/term | v0.45.0 | BSD-3-Clause (+ Go patent grant) |
| golang.org/x/text | v0.36.0 | BSD-3-Clause (+ Go patent grant) |
| google.golang.org/genproto/googleapis/rpc | v0.0.0-20260414002931-afd174a4e478 | Apache-2.0 |
| google.golang.org/grpc | v1.82.1 | Apache-2.0 |
| google.golang.org/protobuf | v1.36.11 | BSD-3-Clause (+ Go patent grant) |

### Apache-2.0 modules

Governed by the **Apache License, Version 2.0** reproduced below. Attribution:

- **github.com/spf13/cobra** — Copyright The Cobra Authors.
- **github.com/inconshreveable/mousetrap** — Copyright Alan Shreve (inconshreveable).
- **google.golang.org/genproto/googleapis/rpc** — Copyright Google LLC.
- **google.golang.org/grpc** — Copyright 2014 gRPC authors. Its `NOTICE`:

  ```
  Copyright 2014 gRPC authors.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  ```

### BSD-3-Clause modules

Governed by the **3-clause BSD license** reproduced below. The license body is
identical across these modules apart from the copyright line and the Google
entity name; each module's exact copyright notice:

- **golang.org/x/net**, **golang.org/x/sys**, **golang.org/x/term**,
  **golang.org/x/text** — `Copyright 2009 The Go Authors.` (name: Google LLC).
- **google.golang.org/protobuf** — `Copyright (c) 2018 The Go Authors. All rights reserved.` (name: Google Inc.).
- **github.com/spf13/pflag** — `Copyright (c) 2012 Alex Ogier. All rights reserved.` and `Copyright (c) 2012 The Go Authors. All rights reserved.` (name: Google Inc.).

The `golang.org/x/*` and `google.golang.org/protobuf` modules additionally carry
the Go project's **Additional IP Rights Grant (Patents)**, reproduced below;
`github.com/spf13/pflag` does not.

### MIT modules

- **github.com/BurntSushi/toml** — under the **MIT License** reproduced below,
  `Copyright (c) 2013 TOML authors`.

---

## Apache License, Version 2.0

Applies to the adapted `retry` code above and to the Apache-2.0 modules listed
under "Binary dependencies".

```
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the
      purposes of this License, Derivative Works shall not include works
      that remain separable from, or merely link (or bind by name) to the
      interfaces of, the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including the
      original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS
```

---

## 3-clause BSD License

Applies to the BSD-3-Clause modules listed under "Binary dependencies". The text
below is `golang.org/x/*`'s (name: Google LLC); `google.golang.org/protobuf` and
`github.com/spf13/pflag` are word-for-word identical except that the copyright
lines are as listed above and the entity name reads "Google Inc." Each module's
own copyright notice governs it.

```
Copyright 2009 The Go Authors.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google LLC nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### Additional IP Rights Grant (Patents)

Ships with `golang.org/x/net`, `golang.org/x/sys`, `golang.org/x/term`,
`golang.org/x/text`, and `google.golang.org/protobuf`.

```
Additional IP Rights Grant (Patents)

"This implementation" means the copyrightable works distributed by
Google as part of the Go project.

Google hereby grants to You a perpetual, worldwide, non-exclusive,
no-charge, royalty-free, irrevocable (except as stated in this section)
patent license to make, have made, use, offer to sell, sell, import,
transfer and otherwise run, modify and propagate the contents of this
implementation of Go, where such license applies only to those patent
claims, both currently owned or controlled by Google and acquired in
the future, licensable by Google that are necessarily infringed by this
implementation of Go.  This grant does not include claims that would be
infringed only as a consequence of further modification of this
implementation.  If you or your agent or exclusive licensee institute or
order or agree to the institution of patent litigation against any
entity (including a cross-claim or counterclaim in a lawsuit) alleging
that this implementation of Go or any code incorporated within this
implementation of Go constitutes direct or contributory patent
infringement, or inducement of patent infringement, then any patent
rights granted to you under this License for this implementation of Go
shall terminate as of the date such litigation is filed.
```

---

## MIT License

Applies to the MIT modules listed under "Binary dependencies", under each
module's own copyright notice.

```
The MIT License (MIT)

Copyright (c) 2013 TOML authors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```
