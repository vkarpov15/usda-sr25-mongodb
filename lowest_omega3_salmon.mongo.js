db.nutrition.aggregate([
  {
    // Only care about foods that contain 'salmon'
    $match : { description : /salmon/ig }
  },
  {
    // Generate a document for every value of nutrients
    $unwind : "$nutrients"
  },
  {
    // Only match omega-3 fatty acids
    $match : { 'nutrients.description' : /n-3/ig }
  },
  {
    // Transform the data set into documents where the _id
    // is the food description and `omega3` is the sum of
    // all the omega-3 fatty acid amounts
    $group : {
      _id : "$description",
      omega3 : { $sum : "$nutrients.amountPer100G" }
    }
  },
  {
    $sort : { omega3 : 1 }
  },
  {
    $limit : 1
  }
]);

