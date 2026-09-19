# Example output

Three runs of the same build (feedbench 0.1.0, `cargo test`: 17 passed), captured on 2026-09-19.
Host names and file paths in the headers were replaced; every number is as printed. These runs
predate the `p10`/`p90` columns of the block-size table.

## Run 1: a host whose address was already at the official feed's connection limit, 120 s

`cargo test --release` on the host passed (17/17) right before the run. The host is admitted
to Eira Pulse; the official feed answered every handshake from this address with `429 Too Many
Requests` for the whole run (the address already held its allowed connections), so no block or
transaction could be matched here. The output shows what the tool prints in that
case: the Eira source delivered 1182 blocks / 7410 transactions in the 120 s, the window with
every source connected is 0 s, and every table is empty.

```
$ feedbench --source official=wss://feed.mainnet.chain.robinhood.com --eira pulse.eiranodes.dev:443 --seconds 120
feedbench 0.1.0  chain id 4663  2 source(s)  120 s  stamp=Arrival
  official   nitro   wss://feed.mainnet.chain.robinhood.com
  eira       eira    pulse.eiranodes.dev:443  level RECEIVED
+    0.0s [eira] connected level=RECEIVED filters=1
[official] connect failed: handshake rejected: HTTP/1.1 429 Too Many Requests; next attempt in 45 s
[official] connect failed: handshake rejected: HTTP/1.1 429 Too Many Requests; next attempt in 45 s
[official] connect failed: handshake rejected: HTTP/1.1 429 Too Many Requests; next attempt in 45 s

feedbench 0.1.0  Robinhood Chain (chain id 4663)
start    2026-09-19 08:23:00 UTC
end      2026-09-19 08:25:00 UTC   duration 120.0 s
host     node-host
stamp    arrival (message in memory, before inflating and parsing)   warmup 5 s   min-sources 2
sources
  official   nitro   wss://feed.mainnet.chain.robinhood.com
                reconnects 0   blocks 0   txs 0 (0.0 MB raw)
  eira       eira    pulse.eiranodes.dev:443   level RECEIVED
             level=RECEIVED filters=1   reconnects 0   blocks 1182   txs 7410 (7.5 MB raw)
window   0.0 s with every source connected   matched blocks 0   matched txs 0
note     Timestamps are taken at message arrival, before decoding, for every source. Eira's already-decoded events are therefore compared on equal terms with raw feeds whose consumer still has to decode them.

A. Block level: first source to deliver a block wins; delay behind the winner in ms
whole run completed blocks: 0
source    wins  win%   p10   p50   p75   p90   p95   p99  p99.9   max  blocks
-----------------------------------------------------------------------------
official     0  0.0%  0.00  0.00  0.00  0.00  0.00  0.00   0.00  0.00       0
eira         0  0.0%  0.00  0.00  0.00  0.00  0.00  0.00   0.00  0.00       0

B. Transaction level: key = tx hash; lag behind the earliest source in ms
distinct txs 0   seen by all 0   flagged swaps 0
source    seen  coverage  first  first>=1ms   p50   p90   p95   p99   max
-------------------------------------------------------------------------
official     0      0.0%   0.0%        0.0%  0.00  0.00  0.00  0.00  0.00
eira         0      0.0%   0.0%        0.0%  0.00  0.00  0.00  0.00  0.00

C1. eira vs official: 0 txs seen by both; positive = eira earlier
median    p10    p90  eira first  within 1ms  eira >5ms earlier  official >5ms earlier
--------------------------------------------------------------------------------------
+0.00   +0.00  +0.00        0.0%        0.0%               0.0%                   0.0%
by block size (tx count from nitro frame):
block size  blocks  blocks%  txs  txs%  eira earlier by  eira first
-------------------------------------------------------------------
1-5 txs          0     0.0%    0  0.0%         +0.00 ms        0.0%
6-20 txs         0     0.0%    0  0.0%         +0.00 ms        0.0%
21-50 txs        0     0.0%    0  0.0%         +0.00 ms        0.0%
>50 txs          0     0.0%    0  0.0%         +0.00 ms        0.0%

percentiles: nearest rank; ms with two decimals; win% and first% over matched blocks/txs
json written to example-node.json
```

## Run 2: two connections to the official feed from a laptop, 120 s

Neither Eira nor a second feed was reachable from this machine, so the run compares two
independent connections to the official feed. It shows every table populated (block level,
transaction level, pair, per-interval medians, block-size buckets). Numbers from a laptop far
from the sequencer are not representative of anything but that laptop; the two connections
differ by tens of milliseconds from each other, which is the spread the tool exists to show.

```
$ feedbench --source official=wss://feed.mainnet.chain.robinhood.com --source official-2=wss://feed.mainnet.chain.robinhood.com --seconds 120 --summary-interval 60
feedbench 0.1.0  chain id 4663  2 source(s)  120 s  stamp=Arrival
  official   nitro   wss://feed.mainnet.chain.robinhood.com
  official-2 nitro   wss://feed.mainnet.chain.robinhood.com
+    1.4s [official-2] connected compression=Arbitrum-permessage-deflate
+    1.6s [official] connected compression=Arbitrum-permessage-deflate

rolling block summary +0s..+60s completed blocks: 534
source      wins   win%   p10    p50    p75    p90     p95     p99   p99.9     max  blocks
------------------------------------------------------------------------------------------
official     168  31.5%  0.00  15.65  48.61  79.21  103.80  142.74  239.70  239.70     534
official-2   366  68.5%  0.00   0.00   5.88  35.47   54.55  108.61  145.33  145.33     534


rolling block summary +60s..+120s completed blocks: 591
source      wins   win%   p10   p50    p75    p90    p95     p99   p99.9     max  blocks
----------------------------------------------------------------------------------------
official     262  44.3%  0.00  5.52  40.72  70.53  89.89  140.68  185.91  185.91     591
official-2   329  55.7%  0.00  0.00  22.59  59.38  79.04  131.34  163.79  163.79     591


feedbench 0.1.0  Robinhood Chain (chain id 4663)
start    2026-09-19 08:22:35 UTC
end      2026-09-19 08:24:35 UTC   duration 120.0 s
host     laptop
stamp    arrival (message in memory, before inflating and parsing)   warmup 5 s   min-sources 2
sources
  official   nitro   wss://feed.mainnet.chain.robinhood.com
             compression=Arbitrum-permessage-deflate   reconnects 0   blocks 1176   txs 7308 (7.3 MB raw)   rx 2.8 MB
  official-2 nitro   wss://feed.mainnet.chain.robinhood.com
             compression=Arbitrum-permessage-deflate   reconnects 0   blocks 1178   txs 7316 (7.4 MB raw)   rx 2.8 MB
window   113.4 s with every source connected   matched blocks 1125   matched txs 7086
note     Timestamps are taken at message arrival, before decoding, for every source. Eira's already-decoded events are therefore compared on equal terms with raw feeds whose consumer still has to decode them.

A. Block level: first source to deliver a block wins; delay behind the winner in ms
whole run completed blocks: 1125
source      wins   win%   p10    p50    p75    p90    p95     p99   p99.9     max  blocks
-----------------------------------------------------------------------------------------
official     430  38.2%  0.00  10.13  44.08  74.21  98.84  141.61  190.21  239.70    1125
official-2   695  61.8%  0.00   0.00  15.68  50.41  69.94  112.49  151.95  163.79    1125

B. Transaction level: key = tx hash; lag behind the earliest source in ms
distinct txs 7088   seen by all 7086   flagged swaps 0
source      seen  coverage  first  first>=1ms    p50    p90    p95     p99     max
----------------------------------------------------------------------------------
official    7086    100.0%  39.6%       37.8%  10.74  72.23  93.03  147.29  239.70
official-2  7088    100.0%  60.4%       57.0%   0.00  51.25  75.99  112.49  163.79

C1. official vs official-2: 7086 txs seen by both; positive = official earlier
median     p10     p90  official first  within 1ms  official >5ms earlier  official-2 >5ms earlier
--------------------------------------------------------------------------------------------------
-10.74  -72.23  +51.25           39.6%        5.2%                  35.0%                    54.8%
interval  from   txs  median  official first
--------------------------------------------
#1          0s  3344  -16.48           31.6%
#2         60s  3742   -3.20           46.8%
by block size (tx count from nitro frame):
block size  blocks  blocks%   txs   txs%  official earlier by  official first
-----------------------------------------------------------------------------
1-5 txs        694    62.3%  2223  31.4%            -11.44 ms           35.8%
6-20 txs       383    34.4%  3314  46.8%            -11.67 ms           39.4%
21-50 txs       29     2.6%   852  12.0%             -8.35 ms           46.7%
>50 txs          8     0.7%   697   9.8%             -0.06 ms           44.5%

percentiles: nearest rank; ms with two decimals; win% and first% over matched blocks/txs
```

## Run 3: VPS in Europe (Lithuania), official feed against Eira RECEIVED, 180 s, 19 Sep 2026 08:27 UTC

Source IP allowlisted for Eira with a 4-stream plan; the official feed had free connection slots for this address.

```
feedbench 0.1.0  Robinhood Chain (chain id 4663)
start    2026-09-19 08:27:01 UTC
end      2026-09-19 08:30:01 UTC   duration 180.0 s
host     vps-eu
stamp    arrival (message in memory, before inflating and parsing)   warmup 10 s   min-sources 2
sources
  official   nitro   wss://feed.mainnet.chain.robinhood.com
             compression=Arbitrum-permessage-deflate   reconnects 0   blocks 1777   txs 12593 (11.5 MB raw)   rx 4.4 MB
  eira       eira    pulse.eiranodes.dev:443   level RECEIVED
             level=RECEIVED filters=1   reconnects 0   blocks 1760   txs 12593 (11.5 MB raw)
window   169.8 s with every source connected   matched blocks 1672   matched txs 11998
note     Timestamps are taken at message arrival, before decoding, for every source. Eira's already-decoded events are therefore compared on equal terms with raw feeds whose consumer still has to decode them.

A. Block level: first source to deliver a block wins; delay behind the winner in ms
whole run completed blocks: 1672
source    wins   win%   p10    p50    p75    p90    p95    p99  p99.9    max  blocks
------------------------------------------------------------------------------------
official    15   0.9%  7.96  12.83  15.36  21.57  32.50  46.66  97.78  99.04    1672
eira      1657  99.1%  0.00   0.00   0.00   0.00   0.00   0.00   9.22  17.52    1672

B. Transaction level: key = tx hash; lag behind the earliest source in ms
distinct txs 11998   seen by all 11998   flagged swaps 3600
source     seen  coverage  first  first>=1ms    p50    p90    p95    p99    max
-------------------------------------------------------------------------------
official  11998    100.0%   0.6%        0.3%  15.26  57.45  74.07  97.14  99.04
eira      11998    100.0%  99.4%       99.2%   0.00   0.00   0.00   0.00  17.54

C1. eira vs official: 11998 txs seen by both; positive = eira earlier
median    p10     p90  eira first  within 1ms  eira >5ms earlier  official >5ms earlier
---------------------------------------------------------------------------------------
+15.27  +8.70  +57.45       99.4%        0.5%              97.3%                   0.1%
by block size (tx count from nitro frame):
block size  blocks  blocks%   txs   txs%  eira earlier by  eira first
---------------------------------------------------------------------
1-5 txs       1070    64.0%  3464  28.9%        +12.63 ms       98.8%
6-20 txs       528    31.6%  4290  35.8%        +13.37 ms       99.2%
21-50 txs       53     3.2%  1839  15.3%        +19.69 ms      100.0%
>50 txs         21     1.3%  2405  20.0%        +54.23 ms      100.0%

percentiles: nearest rank; ms with two decimals; win% and first% over matched blocks/txs
```
