package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;


import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";


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
					"PRIMARY KEY ((reviewerID), ts, asin)"	+
					") "							+
					"WITH CLUSTERING ORDER BY (ts DESC, reviewerID ASC)";

	// TODO - the ITEM_REVIEWS_BY_TIME_reviewerID does not match the tables, need to fix?
	private static final String		ITEM_REVIEWS_BY_TIME_reviewerID =
			"SELECT * FROM " + TABLE_ITEM_REVIEWS + " WHERE reviewerID = ? " +
					"ORDER BY ts DESC, reviewerID ASC";

	private static final String		CQL_TABLE_ITEM_REVIEWS_INSERT =
			"INSERT INTO " + TABLE_USER_REVIEWS + "(ts, asin, reviewerID, reviewerName, overall, summary, reviewText) " +
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
		for (int i=0;i<createTablesList.length;i++) {
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
		itemReviewsByTimeReviewerID = session.prepare(ITEM_REVIEWS_BY_TIME_reviewerID); // TODO - last query not finished
		System.out.println("prepared stmt initialized :))");
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
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
		{
			// you should return the item's description based on the formatItem function.
			// if it does not exist, return the string "not exists"
			// example for asin B005QB09TU

			if (true) // if exists
				item = formatItem(
						"B005QB09TU",
						"Circa Action Method Notebook",
						"http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg",
						new TreeSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")),
						"Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991."
				);
		}
		return item;
	}


	@Override
	public Iterable<String> userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		ArrayList<String> reviews = new ArrayList<String>();
		BoundStatement boundUserReviews = userReviewsByTimeAsin.bind().setString("reviewerID", reviewerID);
		ResultSet userReviews = session.execute(boundUserReviews);
		Row row = userReviews.one();
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
			row = userReviews.one();
			count++;
		}
		{
			// required format - example for reviewerID A17OJCRPMYWXWV
			ArrayList<String> reviewRepers = new ArrayList<String>();
			String reviewRepr1 = formatReview(
					Instant.ofEpochSecond(1362614400),
					"B005QDG2AI",
					"A17OJCRPMYWXWV",
					"Old Flour Child",
					5,
					"excellent quality",
					"These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!"
			);
			reviewRepers.add(reviewRepr1);

			String reviewRepr2 = formatReview(
					Instant.ofEpochSecond(1360108800),
					"B003I89O6W",
					"A17OJCRPMYWXWV",
					"Old Flour Child",
					5,
					"Checkbook Cover",
					"Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy."
			);
			reviewRepers.add(reviewRepr2);
		}
		System.out.println("total reviews: " + count);
		return reviews;
	}


	@Override
	public Iterable<String> itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		// required format - example for asin B005QDQXGQ
		ArrayList<String> reviewRepers = new ArrayList<String>();
		reviewRepers.add(
			formatReview(
				Instant.ofEpochSecond(1391299200),
				"B005QDQXGQ",
				"A1I5J5RUJ5JB4B",
				"T. Taylor \"jediwife3\"",
				5,
				"Play and Learn",
				"The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it."
			)
		);

		reviewRepers.add(
			formatReview(
				Instant.ofEpochSecond(1390694400),
				"B005QDQXGQ",
				"\"AF2CSZ8IP8IPU\"",
				"Corey Valentine \"sue\"",
				1,
			 	"Not good",
				"This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy"
			)
		);
		
		reviewRepers.add(
			formatReview(
				Instant.ofEpochSecond(1388275200),
				"B005QDQXGQ",
				"A27W10NHSXI625",
				"Beth",
				2,
				"Way overpriced for a beach ball",
				"It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway."
			)
		);

		System.out.println("total reviews: " + 3);
		return reviewRepers;
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
