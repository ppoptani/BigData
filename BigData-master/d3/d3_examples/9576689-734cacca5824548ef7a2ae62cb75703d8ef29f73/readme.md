This is a chart as used as an example to demonstrate the integration of a [YQL](http://developer.yahoo.com/yql/) query with [d3.js](http://d3js.org/)  and incorperating a degree of control over the query. It is described in the book [D3 Tips and Tricks](https://leanpub.com/D3-Tips-and-Tricks).

You can enter a stock in the form of a [ticker symbol](http://en.wikipedia.org/wiki/Ticker_symbol) (YHOO = Yahoo!, MSFT = Microsoft, KO = Coca-Cola) along with a start and end date (in the format yyyy-mm-dd).

Fair Warning: There is no degree of sanitisation or validation on the inputs. The purpose is to demonstrate the YQL integration with d3.js.

**Interesting note:** The original version of this example used 'GOOG' as the ticker symbol, but for whatever reason it [no longer appears to work](http://stackoverflow.com/questions/22150029/option-symbol-goog-not-working-in-yql). 