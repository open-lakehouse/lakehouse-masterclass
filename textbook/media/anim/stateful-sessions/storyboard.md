# Animation: transformWithState keeps each customer's open visit in the state store and closes it with an event-time timer

**Claim.** transformWithState keeps each customer's open visit in the state store and closes it with an event-time timer once the watermark passes the quiet gap. The scene follows `lakehouse/customer_sessions.py` (course repo, Chapter 8) line for line: ValueState `visit` (start_ms, last_ms), MapState `orders` keyed by order_id, timer at last_ms + SESSION_GAP_MS, watermark from `withWatermark("created_at", WATERMARK)`, output row appended to `iceberg.gold.customer_sessions` with revenue summed over orders that are not cancelled. The newer out-of-order rule (an order up to one gap before start moves start back; older ones are skipped, only before a watermark exists) is in the scene's logic but has no beat.

**Numbers.** `data.json`. SESSION_GAP_MS = 15000 and WATERMARK = "10 seconds" are read from `customer_sessions.py`. Event times, order ids, amounts and statuses are schematic and labelled; the state values, timers, watermark positions and the output row are computed in the scene from those inputs by the same rules as the code.

**Backend:** hf (lanes, dots, text panels). **Themes:** paper, ide. **Formats:** portrait, square, landscape. **Camera cut:** yes.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, two customer lanes on an event-time axis, two empty state panels, the watermark at 0, all at frame 0 | visits live in the state store | Each customer's open visit lives in the state store. | names the state before anything arrives |
| `orders` | 4.0 | o-1 and o-2 for c-1, o-7 for c-2 land; visit, timer and orders rows fill; c-1's timer marker moves 17 s to 21 s | each order: last_ms, orders[order_id], timer = last_ms + 15 s | Each order updates the visit, its map entry and the timer. | the three pieces of state, and the timer moving with each order |
| `dup` | 10.0 | a re-sent o-2 drops onto the o-2 dot; the orders[o-2] row pulses, nothing else changes | re-sent o-2 writes key o-2 again: nothing changes | A re-sent order lands on the same map key. Nothing changes. | duplicates are absorbed by the MapState key, no dedup operator |
| `more` | 15.0 | o-3 (c-1, timer to 24 s), o-8 and o-9 (c-2); the watermark moves to 8 s, then 20 s | watermark = latest event time − 10 s | The watermark trails the latest event time by ten seconds. | sets up what moves the clock |
| `close` | 21.0 | o-10 for c-2; watermark to 26 s, past c-1's timer at 24 s; marker flashes; the row appears under iceberg.gold.customer_sessions; c-1's panel clears | watermark passes c-1's timer: one row out, c-1 state cleared | When it passes customer one's timer, the visit closes: one row goes to Iceberg and that key's state is cleared. | the claim itself |
| `late` | 29.0 | o-4 stamped 20 s arrives for c-1, greys out, labelled dropped, fades | o-4 at 20 s is behind the watermark: dropped | An order stamped behind the watermark is dropped. | lateness has a cost, and the watermark sets it |
| `end` | 34.0 | hold; c-2's timer marker sits at 51 s | c-2 stays open until the watermark passes 51 s (stays) | Customer two's visit stays open until the watermark passes fifty-one seconds. | a visit with a recent order is still open |

## What has to be readable

The watermark label at each position; both timer markers; every row in both panels (start_ms, last_ms, timer, orders[...]); the output row (orders 3, revenue 20.50 since cancelled orders are not counted, cancelled 1, closed_by timer); the late tag; the footnote naming the source of 15 s and 10 s.
