fetch('/api/autotrade', {
  method:'POST',
  headers:{'Content-Type':'application/json','Authorization':'Bearer Bambip49'},
  body:JSON.stringify({manualRun:true})
}).then(r=>r.json()).then(d=>console.log(JSON.stringify(d).substring(0,500)))
Promise {<pending>}
238links.js:9164 Devyce Chrome Extension - page not supported for call button parsing.
VM106:1  POST https://ig-dashboard-roan.vercel.app/api/autotrade 500 (Internal Server Error)
(anonymous) @ VM106:1
VM106:5 {"error":"openCount is not defined","stack":"    at module.exports (/var/task/ig-dashboard/api/autotrade.js:1184:26)","log":[]}
8links.js:9164 Devyce Chrome Extension - page not supported for call button parsing.
(index):588  GET https://ig-dashboard-roan.vercel.app/ig/markets/CS.D.USDJPY.TODAY.IP 403 (Forbidden)
igFetch @ (index):588
fetchIGSnapshots @ (index):1199
425links.js:9164 Devyce Chrome Extension - page not supported for call button parsing.
2(index):1136 [LS] Status: DISCONNECTED:WILL-RETRY
2(index):1136 [LS] Status: CONNECTING
(index):1136 [LS] Status: CONNECTED:WS-STREAMING
4links.js:9164 Devyce Chrome Extension - page not supported for call button parsing.
(index):1136 [LS] Status: CONNECTED:WS-STREAMING
3links.js:9164 Devyce Chrome Extension - page not supported for call button parsing.
(index):588  GET https://ig-dashboard-roan.vercel.app/ig/markets/CS.D.GBPUSD.TODAY.IP 403 (Forbidden)
igFetch @ (index):588
fetchIGSnapshots @ (index):1199
250links.js:9164 Devyce Chrome Extension - page not supported for call button parsing.
