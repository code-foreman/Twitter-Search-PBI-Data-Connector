// This file contains your Data Connector logic
section Twitter_Search_API;

[DataSource.Kind="Twitter_Search_API", Publish="Twitter_Search_API.Publish"]

//Shared Function
shared Twitter_Search_API.Contents = Value.ReplaceType(SearchExec, SearchType);
   
SearchType= type function (
       Symbol as (type text meta [
                       Documentation.FieldCaption = "Cryptocurrency Symbol",
                       Documentation.FieldDescription = "Enter a crypto symbol to search in Twitter",
                       Documentation.SampleValues = {"BTC", "DOGE"}]),
                       
            #"Number of Tweets" as (type number meta [
                       Documentation.FieldCaption = "Number of Tweetz",
                       Documentation.FieldDescription = "Specify how many tweets to return. If null only 10 tweets will be returned"])) 
                       as table;


SearchExec= (symbol as text, records as number)=>

let

    Result= Get.Request(symbol, records),

    //Parse Data table
    Data= Result{1}[data],
    CryptoSymbol= Result{0},
    #"Converted to Table" = Table.FromList(Data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"public_metrics", "author_id", "created_at", "text", "id"}, {"public_metrics", "author_id", "created_at", "text", "tweet.id"}),
    #"Expanded public_metrics" = Table.ExpandRecordColumn(#"Expanded Column1", "public_metrics", {"retweet_count", "reply_count", "like_count", "quote_count"}, {"retweet_count", "reply_count", "like_count", "quote_count"}),

    //Parse Users table
    Includes = Result{1}[includes],
    Users = Includes[users],
    #"Converted to Table Users" = Table.FromList(Users, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    UserLookUp = Table.ExpandRecordColumn(#"Converted to Table Users", "Column1", {"id", "name", "username"}, {"user.id", "name", "username"}),
    StopWordsList={"ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", 
                "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", 
                "itself", "other", "off", "is", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", 
                "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", 
                "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", 
                "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", 
                "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", 
                "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", 
                "doing", "it", "how", "further", "was", "here", "than"},
    

    //join Data
    JoinedTable = Table.Join(#"Expanded public_metrics", "author_id", UserLookUp, "user.id"),
    SpecifyKeyTable= Table.AddKey(JoinedTable, {"tweet.id", "author_id"}, true),
    #"Removed Columns" = Table.RemoveColumns(SpecifyKeyTable, {"author_id"}),
    #"Reordered Columns1" = Table.ReorderColumns(#"Removed Columns",{"tweet.id", "user.id", "username", "name", "created_at", "text", "retweet_count", "reply_count", "like_count", "quote_count"}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Reordered Columns1", "text", "tweets_cleansed"),
    QueryColumn= Table.AddColumn(#"Duplicated Column", "Symbol",  each CryptoSymbol),
    Lowercased_Text = Table.TransformColumns(QueryColumn,{{"tweets_cleansed", Text.Lower, type text}}),
    ConvertTypes= Table.TransformColumnTypes(Lowercased_Text,{{"reply_count", Int64.Type}, {"like_count", Int64.Type}, {"quote_count", Int64.Type}, {"retweet_count", Int64.Type}}),




    
   

    CleanseTweets= Transform.ReplaceStopWords(ConvertTypes, "tweets_cleansed", StopWordsList)
in
    CleanseTweets;




 Transform.ReplaceStopWords= (sourceTable as table, 
                        sourceColName as text, 
                        stopWords as list, 
                        optional newColumnName as text)=> 
        let
            rowApply= (row as text)=>
                let
                   textToList= Text.Split(row, " "),
                   textCompare= List.Difference(textToList, stopWords),
                   textAggregate= Text.Combine(textCompare, " ")
                in 
                    textAggregate,

            addResultCol= Table.TransformColumns(sourceTable, {sourceColName, each rowApply(_)}, null, MissingField.Ignore)
        in 
            addResultCol;



    
 Get.Request= (cryptoSymbol as text , 
                    numRecords as number) =>
            let
                //"https://api.twitter.com/2/tweets/search/recent?query=BTC"
                baseUrl=  "https://api.twitter.com/2",
                relativePath= "/tweets/search/recent?query=",
                queryString= cryptoSymbol & " lang:en -is:retweet",
                tweetFields="created_at,public_metrics",
                userFields="name",
                userExpansions="author_id",
                numRecords= Text.From(numRecords),
                token= Extension.CurrentCredential()[Key],
                bearerToken=  "Bearer " & Text.From(token),

                //bearerToken=  "Bearer AAAAAAAAAAAAAAAAAAAAAJ72MwEAAAAAyaKkb%2BpghlClsgAclIJyDPZB2hM%3DHAJRO3BI5WW6DxUmglEJ7wRothw9lWx1jtA7JkE92uZRILIpDY",
            
                getRequest= Json.Document(Web.Contents(baseUrl,[
                                                                RelativePath= relativePath,
                                                                Query=  [query= queryString,
                                                                        #"tweet.fields"= tweetFields,
                                                                        #"user.fields"= userFields,
                                                                        #"expansions"= userExpansions,
                                                                        #"max_results"= numRecords],
                                                                Headers= [#"content-type"= "application/json; charset=utf-8",
                                                                            #"Authorization"= bearerToken]
                                                                ])),
                responseObject= {cryptoSymbol, getRequest} //Query and Data table

            in
               responseObject;


// Data Source Kind description
Twitter_Search_API = [
    Authentication = [
        Key = []
        // UsernamePassword = [],
        // Windows = [],
        //Implicit = []
    ],
    Label = Extension.LoadString("DataSourceLabel")
];

// Data Source UI publishing description
Twitter_Search_API.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = Twitter_Search_API.Icons,
    SourceTypeImage = Twitter_Search_API.Icons
];

Twitter_Search_API.Icons = [
    Icon16 = { Extension.Contents("Twitter_Search_API16.png"), Extension.Contents("Twitter_Search_API20.png"), Extension.Contents("Twitter_Search_API24.png"), Extension.Contents("Twitter_Search_API32.png") },
    Icon32 = { Extension.Contents("Twitter_Search_API32.png"), Extension.Contents("Twitter_Search_API40.png"), Extension.Contents("Twitter_Search_API48.png"), Extension.Contents("Twitter_Search_API64.png") }
];
