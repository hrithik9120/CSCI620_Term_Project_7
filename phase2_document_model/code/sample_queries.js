// ============================================================
// MongoDB Validation & Verification Queries
// Project: Reddit May 2015 Document Model
// ============================================================

use("reddit_may2015");

// ------------------------------------------------------------
// 1️⃣ Collection Stats
// ------------------------------------------------------------
print("\n=== COLLECTION COUNTS ===");
printjson({
  Users: db.users.countDocuments(),
  Subreddits: db.subreddits.countDocuments(),
  Posts: db.posts.countDocuments(),
  Comments: db.comments ? db.comments.countDocuments() : "N/A (if not separate)",
  Moderation: db.moderation.countDocuments()
});

// ------------------------------------------------------------
// 2️⃣ Field Validation - Users
// ------------------------------------------------------------
print("\n=== SAMPLE USER DOCUMENT ===");
printjson(db.users.findOne());

print("\n=== USERS WITH BOTH FLAIRS NULL (count + sample) ===");
let nullFlairUsers = db.users.find({
  author_flair_text: { $in: [null, ""] },
  author_flair_css_class: { $in: [null, ""] },
}).limit(3).toArray();
print("Count:", nullFlairUsers.length);
printjson(nullFlairUsers);

// ------------------------------------------------------------
// 3️⃣ Subreddit Integrity
// ------------------------------------------------------------
print("\n=== SAMPLE SUBREDDIT DOCUMENT ===");
printjson(db.subreddits.findOne());

print("\n=== DUPLICATE SUBREDDIT NAMES (count + sample) ===");
let dupSubs = db.subreddits.aggregate([
  { $group: { _id: "$name", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } },
  { $limit: 5 }
]).toArray();
print("Count:", dupSubs.length);
printjson(dupSubs);

// ------------------------------------------------------------
// 4️⃣ Posts Verification
// ------------------------------------------------------------
print("\n=== SAMPLE POST DOCUMENT (with 1 embedded comment) ===");
printjson(db.posts.findOne({}, { comments: { $slice: 1 } }));

print("\n=== COUNT POSTS WITH EMBEDDED COMMENTS ===");
let postsWithComments = db.posts.countDocuments({ comments: { $exists: true, $ne: [] } });
print("Count:", postsWithComments);

print("\n=== AVERAGE NUMBER OF COMMENTS PER POST ===");
let avgComments = db.posts.aggregate([
  { $project: { num_comments: { $size: { $ifNull: ["$comments", []] } } } },
  { $group: { _id: null, avgComments: { $avg: "$num_comments" } } }
]).toArray();
printjson(avgComments);

print("\n=== POSTS WITHOUT AUTHOR FIELD ===");
let postsNoAuthor = db.posts.countDocuments({ author: { $in: [null, ""] } });
print("Count:", postsNoAuthor);

print("\n=== POSTS PER SUBREDDIT (TOP 10) ===");
let postsPerSub = db.posts.aggregate([
  { $group: { _id: "$subreddit.name", postCount: { $sum: 1 } } },
  { $sort: { postCount: -1 } },
  { $limit: 10 }
]).toArray();
printjson(postsPerSub);

// ------------------------------------------------------------
// 5️⃣ Embedded Comments Validation
// ------------------------------------------------------------
print("\n=== TOTAL EMBEDDED COMMENTS (aggregated count) ===");
let totalEmbedded = db.posts.aggregate([
  { $unwind: "$comments" },
  { $count: "total_comments" },
]).toArray();
printjson(totalEmbedded);

print("\n=== SAMPLE COMMENT STRUCTURE (random 2) ===");
let randomComments = db.posts.aggregate([
  { $unwind: "$comments" },
  { $sample: { size: 2 } },
  { $project: { _id: 0, "comments.id": 1, "comments.author": 1, "comments.body": 1 } }
]).toArray();
printjson(randomComments);

print("\n=== COMMENTS WITH HIGH CONTROVERSIALITY (sample 3) ===");
let controversial = db.posts.aggregate([
  { $unwind: "$comments" },
  { $match: { "comments.controversiality": { $gt: 0 } } },
  { $limit: 3 },
  { $project: { "comments.id": 1, "comments.controversiality": 1 } },
]).toArray();
printjson(controversial);

// ------------------------------------------------------------
// 6️⃣ Moderation Data Verification
// ------------------------------------------------------------
print("\n=== SAMPLE MODERATION DOCUMENT ===");
printjson(db.moderation.findOne());

print("\n=== COUNT MODERATIONS WITH REMOVAL REASON ===");
let modCount = db.moderation.countDocuments({ removal_reason: { $ne: null } });
print("Count:", modCount);

print("\n=== TOP 10 SUBREDDITS WITH MOST MODERATIONS ===");
let topModerations = db.moderation.aggregate([
  { $group: { _id: "$subreddit_id", actions: { $sum: 1 } } },
  { $sort: { actions: -1 } },
  { $limit: 10 },
]).toArray();
printjson(topModerations);

print("\n=== POSTS WITH MODERATION RECORDS (count + sample) ===");

// Count how many posts have moderation records
let postsWithModsCount = db.posts.aggregate([
  { $lookup: { from: "moderation", localField: "_id", foreignField: "target_id", as: "mod_records" } },
  { $match: { "mod_records.0": { $exists: true } } },
  { $count: "posts_with_moderation" }
]).toArray();

print("Count of posts with moderation:");
printjson(postsWithModsCount);

// Show a few sample posts that have moderation records
let postsWithModsSamples = db.posts.aggregate([
  { $lookup: { from: "moderation", localField: "_id", foreignField: "target_id", as: "mod_records" } },
  { $match: { "mod_records.0": { $exists: true } } },
  { $limit: 3 },
  { $project: { _id: 1, "subreddit.name": 1, author: 1, "mod_records.removal_reason": 1, "mod_records.distinguished": 1 } }
]).toArray();

print("\nSample posts with moderation records:");
printjson(postsWithModsSamples);

// ------------------------------------------------------------
// 7️⃣ Index Recommendations
// ------------------------------------------------------------
print("\n=== RECOMMENDED INDEXES FOR PERFORMANCE (creating if missing) ===");

let indexes = [
  { collection: "users", index: { _id: 1 } },
  { collection: "subreddits", index: { name: 1 } },
  { collection: "posts", index: { "subreddit.name": 1 } },
  { collection: "posts", index: { "comments.author": 1 } },
  { collection: "moderation", index: { subreddit_id: 1 } },
  { collection: "moderation", index: { target_id: 1 } }
];
indexes.forEach(idx => {
  db[idx.collection].createIndex(idx.index);
});
print("Indexes created or already existed.");

// ------------------------------------------------------------
// 8️⃣ Data Quality Checks
// ------------------------------------------------------------
print("\n=== POSTS MISSING SUBREDDIT REFERENCES ===");
let missingSub = db.posts.find({ "subreddit.id": { $in: [null, ""] } }).limit(3).toArray();
print("Count:", missingSub.length);
printjson(missingSub);

print("\n=== COMMENTS WITH NO BODY TEXT (sample) ===");
let emptyComments = db.posts.aggregate([
  { $unwind: "$comments" },
  { $match: { $or: [ { "comments.body": null }, { "comments.body": "" } ] } },
  { $limit: 3 },
  { $project: { "comments.id": 1, "comments.author": 1, "comments.body": 1 } },
]).toArray();
printjson(emptyComments);

print("\n=== DONE ===");
