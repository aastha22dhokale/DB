✅ ✅ YOUR FINAL RULE CONFIRMED
When CREATE succeeds (1001 / 1004), you want:
✅ Log the OnePAM CREATE RESPONSE into _DI fields ONLY
❌ NEVER populate *_ONEPAM fields for CREATE flow
❌ NOT from CREATE individual
❌ NOT from CREATE postal address
❌ NOT from CREATE relationship
❌ NOT from CREATE marking
❌ NOT even UUID into ONEPAM columns
✅ ONEPAM fields must remain NULL in CREATE flow

✅ ONLY DI fields filled
This is exactly what you specified.
✅ ✅ FULL ACCOUNTING MATRIX (FINAL VERSION – EXACTLY AS YOU WANT)
✅ For ObjectCode 1001 & 1004
SEARCH = NULL (CREATE FLOW)
✅ Log DI fields

❌ Do NOT log _ONEPAM fields at any point

❌ Do NOT log OnePAM snapshots even after OnePam CREATE

✅ DI fields = input CSV

✅ ONEPAM fields = remain NULL
SEARCHSEARCH/IDNETIFY = NOT NULL (UPDATE FLOW)
✅ Log SEARCH/IDNETIFY(WHICHEVER NON NULL) response → into _ONEPAM fields

✅ Perform UPDATE API

✅ Log DI fields at end

✅ Log snapshots for relationship + marking (OnePAM)
✅ For ObjectCode 1002 & 1003
SEARCH = NULL
❌ STOP FLOW

❌ No DI

❌ No ONEPAM

❌ Nothing written in accounting
SEARCH/IDNETIFY = NOT NULL
✅ Update flow

✅ Write search response into ONEPAM fields(SEARCH/IDNETIFY NON NULL RESPONSE)

✅ At end write DI fields

✅ Write relationship & marking snapshots
 
