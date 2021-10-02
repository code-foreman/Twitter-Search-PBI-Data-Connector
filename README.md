# Twitter-Search-API
See a full write up of the development process [HERE](https://www.notion.so/Crypto-Currency-Twitter-Dashboard-358f92af7451422390b7f7484f64b1eb) .

To view the code for the connector see the file Twitter Search API/Twitter_Search_API.pq . To try the connector out, see the installation steps below.

In installation of the data connector, follow these steps...

Prerequisites:
1) You'll need a developer account from Twitter. The data connector accepts the bearer token. https://developer.twitter.com/en/apply-for-access
2) (optional) You'll need a developer account from Nomics API to view price information. https://p.nomics.com/cryptocurrency-bitcoin-api

Installation:

1) Copy the extension file into [Documents]/Power BI Desktop/Custom Connectors.
2) Check the option (Not Recommended) Allow any extension to load without validation or warning in Power BI Desktop (under File | Options and settings | Options | Security | Data Extensions).
3) Restart Power BI Desktop.


The source project is also available for further development in Visual Studio Code. You must have the [Power Query SDK](https://docs.microsoft.com/en-us/power-query/installingsdk) installed to modify the source code. Also Visual Studio Code will need to be installed.
