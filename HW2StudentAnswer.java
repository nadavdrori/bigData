package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.Scanner;
import java.util.HashSet;
import java.io.File;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.json.JSONObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.*;


import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";
	int max_threads = 150;
	private final ExecutorService executor = Executors.newFixedThreadPool(max_threads); // Create a thread pool

	// CQL stuff
	private static final String		TABLE_USER_REVIEWS = "user_reviews_by_rt_then_asin";
	private static final String		TABLE_ITEM_REVIEWS = "item_reviews_by_rt_then_rID";
	private static final String		TABLE_ITEM_SEARCHES = "items_by_asin";

	private static final String		CQL_USER_VIEW_SELECT =
			"SELECT * FROM " + "TODO - insert" + " WHERE user_id = ?";



	// table for user reviews, ordered by time and then asin
	private static final String	CQL_CREATE_TABLE_UR =
			"CREATE TABLE " + TABLE_USER_REVIEWS 	+"(" 	+
					"ts timestamp,"					+
					"asin text,"					+
					"reviewerID text,"				+
					"reviewerName text,"			+
					"overall int,"					+
					"summary text,"					+
					"reviewText text,"				+
					"PRIMARY KEY ((reviewerID), ts, asin)"	+
					") "							+
					"WITH CLUSTERING ORDER BY (ts DESC, asin ASC)";

	private static final String		USER_REVIEWS_BY_TIME_ASIN =
			"SELECT * FROM " + TABLE_USER_REVIEWS + " WHERE reviewerID = ? " +
					"ORDER BY ts DESC, asin ASC";

	private static final String		CQL_TABLE_USER_REVIEWS_INSERT =
			"INSERT INTO " + TABLE_USER_REVIEWS + "(ts, asin, reviewerID, reviewerName, overall, summary, reviewText) " +
					"VALUES(?, ?, ?, ?, ?, ?, ?)";

	// table for item searches
	private static final String	CQL_CREATE_TABLE_ITEMS =
			"CREATE TABLE " + TABLE_ITEM_SEARCHES 	+"("+
					"asin text,"					+
					"title text,"					+
					"imageURL text,"				+
					"categories Set<text>,"			+
					"description text,"				+
					"PRIMARY KEY (asin)"			+
					") ";

	private static final String		ITEM_SEARCH_BY_ASIN =
			"SELECT * FROM " + TABLE_ITEM_SEARCHES + " WHERE asin = ?";

	private static final String		CQL_TABLE_ITEM_SEARCHES_INSERT =
			"INSERT INTO " + TABLE_ITEM_SEARCHES + "(asin, title, imageURL, categories, description) " +
					"VALUES(?, ?, ?, ?, ?)";

	private static final String	CQL_CREATE_TABLE_IR =
			"CREATE TABLE " + TABLE_ITEM_REVIEWS 	+"(" 	+
					"ts timestamp,"					+
					"asin text,"					+
					"reviewerID text,"				+
					"reviewerName text,"			+
					"overall int,"					+
					"summary text,"					+
					"reviewText text,"				+
					"PRIMARY KEY ((asin), ts, reviewerID)"	+
					") "							+
					"WITH CLUSTERING ORDER BY (ts DESC, reviewerID ASC)";

	private static final String		ITEM_REVIEWS_BY_TIME_reviewerID =
			"SELECT * FROM " + TABLE_ITEM_REVIEWS + " WHERE asin = ? " +
					"ORDER BY ts DESC, reviewerID ASC";

	private static final String		CQL_TABLE_ITEM_REVIEWS_INSERT =
			"INSERT INTO " + TABLE_ITEM_REVIEWS + "(ts, asin, reviewerID, reviewerName, overall, summary, reviewText) " +
					"VALUES(?, ?, ?, ?, ?, ?, ?)";


	// global const
	public static final String[] createTablesList = {CQL_CREATE_TABLE_UR, CQL_CREATE_TABLE_ITEMS, CQL_CREATE_TABLE_IR};
	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	private PreparedStatement stmtUserReviews;
	private PreparedStatement stmtItemSearches;
	private PreparedStatement stmtItemReviews;
	private PreparedStatement itemSearchByAsin;
	private PreparedStatement userReviewsByTimeAsin;
	private PreparedStatement itemReviewsByTimeReviewerID;
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	
	@Override
	public void createTables() {
		for (int i=0; i<createTablesList.length; i++) {
			session.execute(createTablesList[i]);
		}
		System.out.println("Tables crated successfully :)");
	}

	@Override
	public void initialize() {
		stmtUserReviews = session.prepare(CQL_TABLE_USER_REVIEWS_INSERT);
		stmtItemSearches = session.prepare(CQL_TABLE_ITEM_SEARCHES_INSERT);
		stmtItemReviews = session.prepare(CQL_TABLE_ITEM_REVIEWS_INSERT);
		itemSearchByAsin = session.prepare(ITEM_SEARCH_BY_ASIN);
		userReviewsByTimeAsin = session.prepare(USER_REVIEWS_BY_TIME_ASIN);
		itemReviewsByTimeReviewerID = session.prepare(ITEM_REVIEWS_BY_TIME_reviewerID);
		System.out.println("prepared stmts initialized :))");
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		try {
			Scanner scanner = new Scanner(new File(pathItemsFile));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine().trim();
				if (!line.isEmpty()) {
					JSONObject item = new JSONObject(line); // Parse JSON object
					insertToItemSearches(item);
				}
			}
			try {
				// Gracefully shut down the executor and wait indefinitely for tasks to finish
				executor.shutdown();
				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (Exception e) {
				// Handle exception
			}
			scanner.close();
			System.out.println("All items inserted successfully!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void insertToItemSearches(JSONObject item) {
		// Submit the task to the executor
		executor.submit(() -> {
			try {
				String asin = item.getString("asin");
				String title = item.optString("title", NOT_AVAILABLE_VALUE);
				String imageURL = item.optString("imageURL", NOT_AVAILABLE_VALUE);
				Set<String> categories = new HashSet<>();

				// Check if categories exist in the JSON and add to the set
				if (item.has("categories") && !item.isNull("categories")) {
					item.getJSONArray("categories").forEach(cat -> categories.add(cat.toString()));
				} else {
					categories.add(NOT_AVAILABLE_VALUE); // Set to default value if missing
				}

				String description = item.optString("description", NOT_AVAILABLE_VALUE); // Handle missing values

				// Execute the insert into Cassandra
				session.execute(stmtItemSearches.bind()
						.setString("asin", asin)
						.setString("title", title)
						.setString("imageURL", imageURL)
						.setSet("categories", categories, String.class)
						.setString("description", description));
			} catch (Exception e) {
				// Handle exceptions (in this case, do nothing as per original code)
				// Optionally log the error for troubleshooting
				// e.printStackTrace();
			}
		});
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		try {;
			Scanner scanner = new Scanner(new File(pathReviewsFile));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine().trim();
				if (!line.isEmpty()) {
					JSONObject item = new JSONObject(line); // Parse JSON object
					insertReview(item);
				}
			}
			scanner.close();
			// Shutdown executor and wait for completion after all tasks have been submitted
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
//			if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
//				System.out.println("Executor did not finish in time.");
//			}

			System.out.println("All reviews inserted successfully!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void insertReview(JSONObject item) {
		try {
			// Extract values from JSON
			Instant currentTimestamp = Instant.now(); // Current timestamp
			String asin = item.optString("asin", NOT_AVAILABLE_VALUE);
			String reviewerID = item.optString("reviewerID", NOT_AVAILABLE_VALUE);
			String reviewerName = item.optString("reviewerName", NOT_AVAILABLE_VALUE);
			int overall = item.optInt("overall", -1); // Default to -1 if missing
			String summary = item.optString("summary", NOT_AVAILABLE_VALUE);
			String reviewText = item.optString("reviewText", NOT_AVAILABLE_VALUE);

			if (!(reviewerID.equals(NOT_AVAILABLE_VALUE) || asin.equals(NOT_AVAILABLE_VALUE))) {
				// reviewerID, asin is part of primary key in UserReviews, ItemReviews
				// Insert data into Cassandra
				// Submit two tasks asynchronously
				executor.submit(() -> {
					try {
						session.execute(stmtUserReviews.bind()
								.setInstant("ts", currentTimestamp)
								.setString("asin", asin)
								.setString("reviewerID", reviewerID)
								.setString("reviewerName", reviewerName)
								.setInt("overall", overall)
								.setString("summary", summary)
								.setString("reviewText", reviewText)
						);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});

				executor.submit(() -> {
					try {
						session.execute(stmtItemReviews.bind()
								.setInstant("ts", currentTimestamp)
								.setString("asin", asin)
								.setString("reviewerID", reviewerID)
								.setString("reviewerName", reviewerName)
								.setInt("overall", overall)
								.setString("summary", summary)
								.setString("reviewText", reviewText)
						);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String item(String asin) {
		String item = "not exists";	// if not exists
		BoundStatement boundItemData = itemSearchByAsin.bind().setString("asin", asin);
		ResultSet itemData = session.execute(boundItemData);
		Row row = itemData.one();
		if (row != null) {// if exists
			item = formatItem(
					row.getString("asin"),
					row.getString("title"),
					row.getString("imageURL"),
					row.getSet("categories", String.class),
					row.getString("description")
			);
		}
		return item;
	}


	@Override
	public Iterable<String> userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		ArrayList<String> reviews = new ArrayList<String>();
		BoundStatement boundUserReviews = userReviewsByTimeAsin.bind().setString("reviewerID", reviewerID);
		ResultSet userReviewsSet = session.execute(boundUserReviews);
		Row row = userReviewsSet.one();
		int count = 0;
		while (row != null) {
			String reviewRep = formatReview(
					row.getInstant("ts"),          // timestamp (ts)
					row.getString("asin"),         // asin
					row.getString("reviewerID"),   // reviewerID
					row.getString("reviewerName"), // reviewerName
					row.getInt("overall"),         // overall (rating)
					row.getString("summary"),      // summary
					row.getString("reviewText")    // reviewText
			);
			reviews.add(reviewRep);
			row = userReviewsSet.one();
			count++;
		}
		System.out.println("total reviews: " + count);
		return reviews;
	}


	@Override
	public Iterable<String> itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID

		ArrayList<String> reviews = new ArrayList<String>();
		BoundStatement boundItemReviews = itemReviewsByTimeReviewerID.bind().setString("asin", asin);
		ResultSet itemReviewsSet = session.execute(boundItemReviews);
		Row row = itemReviewsSet.one();
		int count = 0;
		while (row != null) {
			String reviewRep = formatReview(
					row.getInstant("ts"),          // timestamp (ts)
					row.getString("asin"),         // asin
					row.getString("reviewerID"),   // reviewerID
					row.getString("reviewerName"), // reviewerName
					row.getInt("overall"),         // overall (rating)
					row.getString("summary"),      // summary
					row.getString("reviewText")    // reviewText
			);
			reviews.add(reviewRep);
			row = itemReviewsSet.one();
			count++;
		}

		System.out.println("total reviews: " + count);
		return reviews;
	}

	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
		String reviewDesc = 
			"time: " + time + 
			", asin: " 	+ asin 	+
			", reviewerID: " 	+ reviewerId +
			", reviewerName: " 	+ reviewerName 	+
			", rating: " 		+ rating	+ 
			", summary: " 		+ summary +
			", reviewText: " 	+ reviewText + "\n";
		return reviewDesc;
	}

}
