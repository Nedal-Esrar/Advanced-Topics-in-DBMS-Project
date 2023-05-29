1) Create a new database called gamesDB.
```
test> use gamesDB
```

2) Write a query to make sure that you are using the gamesDB.
```
gamesDB> db
```

3) Create a new collection called games, make sure it has been created.
```
gamesDB> db.createCollection("games")
// it will show { ok: 1 } to indicate that collection has been created
```

4) Write query to make sure that the collection was created.
```
gamesDB> show collections
```

5) Add 5 games to the games collection; give each one of them has the following properties: name, publisher, year_released, and rating (value from 1 to 5).
```
gamesDB> db.games.insertMany([ 
...{name: "Minecraft", publisher: "Mojang", year_released: 2010, rating: 4}, 
...{name: "GTA V", publisher: "Rockstar Games", year_released: 2014, rating: 5}, 
...{name: "PUBG", publisher: "Krafton", year_released: 2018, rating: 3},
...{name: "Fortnite", publisher: "Epic Games", year_released: 2017, rating: 2}, 
...{name: "Tom Clancy's Rainbow Six Siege", publisher: "Ubisoft", year_released: 2015, rating: 1}
...]);
```

6) Write a query to return all games in the collection.
```
gamesDB> db.games.find();
```

7) Write a query that return only 3 games.
```
gamesDB> db.games.find().limit(3);
```

8) Write a query to return the top 3 games based on rating value.
```
gamesDB> db.games.find().sort({rating: -1}).limit(3);
```

9) Write a query that return games whose rating is 5 and released after 2007.
```
gamesDB> db.games.find({rating: 5, year_released: {$gt: 2007}});
```

10) Update the game whose rating is 3 to be 4.
```
gamesDB> db.games.updateMany({rating: 3}, {$set: {rating: 4}});
```
