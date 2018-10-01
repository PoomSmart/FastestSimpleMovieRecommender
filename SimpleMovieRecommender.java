
/**
 * Thatchapon Unprasert
 * 5888220
 * Sec.1
 * 
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.util.FastMath;

import com.google.common.collect.HashBiMap;

final class PSUtils {

	/**
	 * Returns the corresponding rating of user u to movie mov.
	 * 
	 * @param mov
	 * @param u
	 * @return
	 */
	public static Rating ratingByUser(Movie mov, User u) {
		if (mov == null || u == null)
			return null;
		return u.ratings.get(mov.mid);
	}

	/**
	 * Checks if user u has rated movie mov, i.e., the corresponding rating of this user exists by using movie object.
	 * 
	 * @param mov
	 * @param u
	 * @return
	 */
	public static boolean hasUserRated(Movie mov, User u) {
		return ratingByUser(mov, u) != null;
	}

	/**
	 * Checks if user u has rated movie mov, i.e., the corresponding rating of this user exists by using movie id.
	 * 
	 * @param mid
	 * @param u
	 * @return
	 */
	public static boolean hasUserRated2(Integer mid, User u) {
		return u.ratings.containsKey(mid);
	}

	/**
	 * Returns the rating score for movie mov of user u, by accessing the corresponding rating of the user, if exists.
	 * Otherwise, return 0.
	 * 
	 * @param mov
	 * @param u
	 * @return
	 */
	public static double ratingScoreByUser(Movie mov, User u) {
		Rating rating = ratingByUser(mov, u);
		if (rating == null)
			return 0;
		return rating.rating;
	}

}

public class SimpleMovieRecommender implements BaseMovieRecommender {

	protected Map<Integer, Movie> movies; // list of movies, sorted
	protected Map<Integer, User> users; // list of users, sorted
	protected HashBiMap<Integer, Integer> uidmap; // 1-1 map for users' uid
	protected HashBiMap<Integer, Integer> midmap; // 1-1 map for movies' mid
	protected double ratingmap[][]; // map for movies rating
	protected double smatrix[][]; // map for user similarity
	protected boolean fatal = false; // value indicating any error during the operation

	protected final static String NUM_USERS = "@NUM_USERS";
	protected final static String USER_MAP = "@USER_MAP";
	protected final static String NUM_MOVIES = "@NUM_MOVIES";
	protected final static String MOVIE_MAP = "@MOVIE_MAP";
	protected final static String RATING_MATRIX = "@RATING_MATRIX";
	protected final static String USERSIM_MATRIX = "@USERSIM_MATRIX";

	protected boolean allow_usersim_matrix = true;

	@Override
	public Map<Integer, Movie> loadMovies(String movieFilename) {
		if (movieFilename == null) {
			// filepath is null
			fatal = true;
			return null;
		}
		FileManager manager = null;
		BufferedReader reader = null;
		try {
			manager = new FileManager(movieFilename, FileManager.Mode.r);
		} catch (Exception e) {
			// file doesn't exist or whatsoever
			manager.cleanup();
			return null;
		}
		reader = manager.getReader();
		Map<Integer, Movie> map = HashBiMap.create();
		String line;
		Pattern p = Pattern.compile("(\\d+),\"*(.+) \\((\\d+)\\)\"*,(.*)");
		try {
			while ((line = reader.readLine()) != null) {
				Matcher m = p.matcher(line);
				if (!m.find())
					continue;
				int mid = NumberUtils.parseInt(m.group(1)); // parse mid
				String title = m.group(2); // parse title
				int year = NumberUtils.parseInt(m.group(3)); // parse year
				String tags = m.group(4); // parse tags
				Movie mov = new Movie(mid, title, year); // create movie object
				if (!tags.equals("(no genres listed)")) { // add tags if there exists
					for (String tag : tags.split("\\|")) // separate by |
						mov.addTag(tag);
				}
				map.put(mid, mov);
			}
		} catch (IOException e) {
			fatal = true;
			manager.cleanup();
			return map;
		}
		manager.cleanup();
		// Sort movie map by mid by using TreeMap
		return new TreeMap<Integer, Movie>(map);
	}

	@Override
	public Map<Integer, User> loadUsers(String ratingFilename) {
		if (ratingFilename == null) {
			fatal = true;
			return null;
		}
		FileManager manager = null;
		BufferedReader reader = null;
		try {
			manager = new FileManager(ratingFilename, FileManager.Mode.r);
		} catch (Exception e) {
			manager.cleanup();
			return null;
		}
		reader = manager.getReader();
		Map<Integer, User> map = HashBiMap.create();
		String line;
		Pattern p = Pattern.compile("(\\d+),(\\d+),(\\d.\\d),(\\d+)");
		try {
			while ((line = reader.readLine()) != null) {
				Matcher m = p.matcher(line);
				if (!m.find())
					continue;
				try {
					int uid = NumberUtils.parseInt(m.group(1)); // parse uid
					int movieid = NumberUtils.parseInt(m.group(2)); // parse mid
					String _rating = m.group(3); // parse rating score
					// Exclusively implemented for double only in format x.x, directly accessing each character to
					// convert into number
					// Because Double.parseDouble() is really slow
					double rating = (double) (_rating.charAt(0) - '0') + 0.1 * (double) (_rating.charAt(2) - '0');
					long timestamp = NumberUtils.parseLong(m.group(4));
					if (!map.containsKey(uid))
						map.put(uid, new User(uid)); // only add user if doesn't exist
					// still add rating detected per line
					map.get(uid).addRating(movies.get(movieid), rating, timestamp);
				} catch (Exception e) {
					continue;
				}
			}
		} catch (IOException e) {
			// can be the error while reading line
			fatal = true;
			manager.cleanup();
			return map;
		}
		manager.cleanup();
		// Sort user map by uid by using TreeMap
		return new TreeMap<Integer, User>(map);
	}

	@Override
	public void loadData(String movieFilename, String userFilename) {
		movies = loadMovies(movieFilename);
		users = loadUsers(userFilename);
	}

	@Override
	public Map<Integer, Movie> getAllMovies() {
		if (movies == null)
			return HashBiMap.create(); // empty map than null
		return movies;
	}

	@Override
	public Map<Integer, User> getAllUsers() {
		if (users == null)
			return HashBiMap.create(); // empty map than null
		return users;
	}

	/**
	 * Calculate the similarity between user u and user v.
	 * 
	 * Instead, given the set of all movies, and intersect by the condition hasUserRated2(Integer, User). So that the
	 * loop manipulates only movies that both u and v have rated.
	 * 
	 * - If either movies, or u, or v is null, similarity is 0
	 * 
	 * - If u and v are identical, similarity is 1 (Taken care by other methods involving with similarity already)
	 * 
	 * - If the denominator is 0, similarity is 0
	 * 
	 * - Otherwise, use the formula
	 * 
	 * @param u
	 * @param v
	 * @param mru
	 * @param mrv
	 * @return
	 */
	public double similarity(User u, User v, double mru, double mrv) {
		if (u == null || v == null)
			return 0;
		double d1 = 0, d2 = 0, answer = 0;
		Collection<Rating> ratings = u.ratings.values();
		for (Rating r : ratings) {
			// This condition mimics the intersection between movies rated by user u and user v,
			// gives much faster performance on large data set
			if (!PSUtils.hasUserRated2(r.m.mid, v))
				continue;
			double ru = r.rating;
			double rv = PSUtils.ratingScoreByUser(r.m, v);
			double dd1 = ru - mru;
			double dd2 = rv - mrv;
			d1 += dd1 * dd1;
			d2 += dd2 * dd2;
			answer += dd1 * dd2;
		}
		if (d1 == 0 || d2 == 0)
			return 0;
		answer /= FastMath.sqrt(d1 * d2);
		if (answer > 1)
			return 1;
		if (answer < -1)
			return -1;
		return answer;
	}

	@Override
	public void trainModel(String modelFilename) {
		if (modelFilename == null || fatal)
			return;
		FileManager manager = null;
		BufferedWriter writer = null;
		try {
			manager = new FileManager(modelFilename, FileManager.Mode.w);
		} catch (Exception e) {
			fatal = true;
			manager.cleanup();
			return;
		}
		writer = manager.getWriter();
		StringBuilder sb = new StringBuilder();
		int usize = users.size(); // total numbers of users
		int msize = movies.size(); // total numbers of movies
		sb.append(NUM_USERS + " " + usize + "\n" + USER_MAP + " {");
		int idx = 0;
		for (Integer uid : users.keySet()) // access uid key set to map them
			sb.append(idx++ + "=" + uid + ", ");
		sb.delete(sb.length() - 2, sb.length()); // delete excess ", "
		sb.append("}\n" + NUM_MOVIES + " " + msize + "\n" + MOVIE_MAP + " {");
		idx = 0;
		for (Integer mid : movies.keySet()) // access mid key set to map them
			sb.append(idx++ + "=" + mid + ", ");
		sb.delete(sb.length() - 2, sb.length());
		sb.append("}\n" + RATING_MATRIX + "\n");
		System.out.println("@@@ Computing user rating matrix");
		idx = 0;
		double meanRating[] = new double[usize]; // array for average rating for all users
		Collection<User> _users = users.values(); // users list
		Collection<Movie> _movies = movies.values(); // movies list
		double sumRating;
		// for-loop for user-movie
		for (User u : _users) {
			sumRating = 0; // will be used to calculate average rating
			for (Movie mov : _movies) {
				double r = PSUtils.ratingScoreByUser(mov, u); // find out rating score that user gave to each movie
				sumRating += r;
				sb.append(r + " ");
			}
			int ratingSize = u.ratings.size();
			if (ratingSize > 0)
				sumRating /= ratingSize;
			// save average rating for each user, to use later with user similarity calculation
			sb.append((meanRating[idx++] = sumRating) + " \n");
		}
		if (allow_usersim_matrix) {
			sb.append(USERSIM_MATRIX + "\n");
			// We can reduce time complexity by half because this is symmetric matrix
			System.out.println("@@@ Computing user sim matrix");
			// similarity matrix
			// it isn't used anywhere else but for value assignment purpose
			smatrix = new double[usize][usize];
			idx = 0;
			int idx2;
			// for-loop for user-user
			for (User u : _users) {
				idx2 = 0;
				for (User v : _users) {
					if (idx == idx2) // or u == v, always 1.0
						sb.append("1.0 ");
					else {
						smatrix[idx][idx2] = (idx2 > idx) ? similarity(u, v, meanRating[idx], meanRating[idx2])
								: smatrix[idx2][idx];
						sb.append(smatrix[idx][idx2] + " ");
					}
					idx2++;
				}
				sb.append("\n");
				idx++;
			}
		}
		System.out.println("@@@ Writing out model file");
		try {
			writer.write(sb.toString());
		} catch (Exception e) {
			System.out.println("@@@ Writing model file failed");
			fatal = true;
		} finally {
			manager.cleanup();
		}
	}

	@Override
	public void loadModel(String modelFilename) {
		if (modelFilename == null || fatal) {
			fatal = true;
			return;
		}
		FileManager manager = null;
		BufferedReader reader = null;
		try {
			manager = new FileManager(modelFilename, FileManager.Mode.r);
		} catch (Exception e) {
			fatal = true;
			manager.cleanup();
			return;
		}
		reader = manager.getReader();
		String line;
		try {
			// Always check pattern or format before reading into each part
			Matcher m = Pattern.compile(NUM_USERS + " (\\d+)").matcher(line = reader.readLine());
			if (!m.find()) {
				System.out.println("@@@ Read " + NUM_USERS + " failed.");
				fatal = true;
				manager.cleanup();
				return;
			}
			int usize = NumberUtils.parseInt(m.group(1)); // get total numbers of users
			uidmap = HashBiMap.create(); // create user map here
			smatrix = new double[usize][usize]; // create similarity matrix here
			m = Pattern.compile(USER_MAP + " \\{(.+)\\}").matcher(line = reader.readLine());
			if (!m.find()) {
				System.out.println("@@@ Read " + USER_MAP + " failed.");
				fatal = true;
				manager.cleanup();
				return;
			}
			int pos, end;
			// Tokenizer for index-id mapping
			StringTokenizer indexToken = new StringTokenizer(m.group(1), ", ");
			int idx = 0;
			while (indexToken.hasMoreTokens()) {
				String pair = indexToken.nextToken();
				// interest only value after '='
				String uidString = pair.substring(pair.indexOf('=') + 1, pair.length());
				int uid = NumberUtils.parseInt(uidString);
				uidmap.put(uid, idx++);
			}
			m = Pattern.compile(NUM_MOVIES + " (\\d+)").matcher(line = reader.readLine());
			if (!m.find()) {
				System.out.println("@@@ Read " + NUM_MOVIES + " failed.");
				fatal = true;
				manager.cleanup();
				return;
			}
			int msize = NumberUtils.parseInt(m.group(1)); // get total numbers of movies
			midmap = HashBiMap.create(); // create movie map here
			ratingmap = new double[usize][msize + 1]; // create rating matrix here
			m = Pattern.compile(MOVIE_MAP + " \\{(.+)\\}").matcher(line = reader.readLine());
			if (!m.find()) {
				System.out.println("@@@ Read " + MOVIE_MAP + " failed.");
				fatal = true;
				manager.cleanup();
				return;
			}
			idx = 0;
			// similar mapping finding to user map
			indexToken = new StringTokenizer(m.group(1), ", ");
			while (indexToken.hasMoreTokens()) {
				String pair = indexToken.nextToken();
				String midString = pair.substring(pair.indexOf('=') + 1, pair.length());
				int mid = NumberUtils.parseInt(midString);
				midmap.put(mid, idx++);
			}
			if (!(line = reader.readLine()).equals(RATING_MATRIX)) {
				System.out.println("@@@ Read " + RATING_MATRIX + " failed.");
				fatal = true;
				manager.cleanup();
				return;
			}
			// read rating matrix, that each value separated by whitespace
			// then read them with this same special method, much faster (\\d\\.\\d)
			for (int i = 0; i < usize; i++) {
				line = reader.readLine();
				int j = 0;
				pos = 0;
				// the mechanism to split string by delimiter ' '
				// find index of ' ' and get substring from beginning position to before ' '
				while ((end = line.indexOf(' ', pos)) >= 0 && j < msize) {
					String _rating = line.substring(pos, end);
					pos = end + 1;
					double rating = (double) (_rating.charAt(0) - '0') + 0.1 * (double) (_rating.charAt(2) - '0');
					ratingmap[i][j] = rating;
					j++;
				}
				// still need the actual double parsing for those averages
				end = line.indexOf(' ', pos);
				ratingmap[i][msize] = NumberUtils.parseDouble(line.substring(pos, end));
			}
			if (allow_usersim_matrix) {
				if (!(line = reader.readLine()).equals(USERSIM_MATRIX)) {
					System.out.println("@@@ Read " + USERSIM_MATRIX + " failed.");
					fatal = true;
					manager.cleanup();
					return;
				}
				for (int i = 0; i < usize; i++) {
					line = reader.readLine();
					// s(u, u) is always 1, not need to actually read the value
					smatrix[i][i] = 1;
					int j = 0;
					pos = 0;
					while ((end = line.indexOf(' ', pos)) >= 0 && j < i) {
						// Using symmetric property of a matrix, we can assign values in parallel, less time complexity
						smatrix[i][j] = smatrix[j][i] = NumberUtils.parseDouble(line.substring(pos, end));
						pos = end + 1;
						j++;
					}
				}
			}
		} catch (IOException e) {
			fatal = true;
		} finally {
			manager.cleanup();
		}
	}

	/**
	 * Return double value of prediction score of the movie for the user.
	 * 
	 * This method is generally faster than predict(Movie, User) because it needs not to keep finding corresponding
	 * indexes for movies and users being used for our caching data; ratingmap[][] and smatrix[][], we can calculate
	 * those indexes once then put them as im and iu.
	 * 
	 * @param mov
	 *            - The movie to be predicted rating score.
	 * @param u
	 *            - The user to be predicted rating score.
	 * @param im
	 *            - Index of movie mov.
	 * @param iu
	 *            - Index of user u.
	 * @return
	 */
	public double _predict(Movie mov, User u, int im, int iu) {
		if (im == -1 || u == null || mov == null)
			return 0;
		if (iu == -1)
			// User u not exist in training (model) file, just get its average rating
			return u.getMeanRating();
		int mmsize = midmap.size();
		double mru = ratingmap[iu][mmsize];
		double d = 0, ans = 0;
		for (int i = 0; i < uidmap.size(); i++) {
			// This condition only selects user that has rated movie at im,
			// i.e., 0.5 <= rating <= 5, also exclude user at iu (user u)
			double rating = ratingmap[i][im];
			if (rating < 0.5 /* || rating > 5 */ || i == iu)
				continue;
			double sim = smatrix[i][iu];
			d += FastMath.abs(sim);
			ans += sim * (rating - ratingmap[i][mmsize]);
		}
		// "d" is denominator of the formula, can also indicate the cardinality of the set; d = 0 iff |N_i| = 0
		// fallback to average rating score if d equals zero
		if (d == 0)
			return mru;
		ans /= d;
		ans += mru;
		// bounds check, only [0, 5] is allowed
		if (ans < 0)
			return 0;
		if (ans > 5)
			return 5;
		return ans;
	}

	@Override
	public double predict(Movie mov, User u) {
		if (mov == null || u == null || fatal)
			return 0;
		// Get index of user u and movie mov, to be used in smatrix[][] and ratingmap[][]
		int iu = uidmap.getOrDefault(u.uid, -1);
		int im = midmap.getOrDefault(mov.mid, -1);
		return _predict(mov, u, im, iu);
	}

	/**
	 * Basically the recommend() method without K, does everything as described in the base file.
	 * 
	 * @param u
	 * @param fromYear
	 * @param toYear
	 * @return
	 */
	public List<MovieItem> _recommend(User u, int fromYear, int toYear) {
		List<MovieItem> fmovies = new Vector<MovieItem>();
		if (fatal || u == null || fromYear > toYear || fromYear < 0 || toYear < 0)
			return fmovies;
		// obtain index for user u here, for performance
		int iu = uidmap.getOrDefault(u.uid, -1);
		int im = 0;
		for (Movie mov : movies.values()) {
			if (mov.year >= fromYear && mov.year <= toYear)
				fmovies.add(new MovieItem(mov, _predict(mov, u, im, iu)));
			im++;
		}
		return fmovies;
	}

	@Override
	public List<MovieItem> recommend(User u, int fromYear, int toYear, int K) {
		if (u == null)
			return new Vector<MovieItem>();
		List<MovieItem> fmovies = _recommend(u, fromYear, toYear);
		if (K <= 1)
			return fmovies;
		Collections.sort(fmovies); // sort before ranking
		if (fmovies.size() > K)
			return fmovies.subList(0, K); // shrink result to size K
		return fmovies;
	}

}

/*
 * Copyright (c) 2015, Laurent Bourges. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 * 
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @author Laurent Bourges
 */
final class NumberUtils {

	public final static int NULL_INT = Integer.MIN_VALUE;
	public final static long NULL_LONG = Long.MIN_VALUE;
	public final static double NULL_DOUBLE = Double.NaN;

	/**
	 * Parses the given value as a double.
	 * 
	 * @param value
	 *            The string representation of the double value.
	 * @return The parsed value or {@link #NULL_DOUBLE} if the value cannot be parsed.
	 */
	public static double parseDouble(final CharSequence value) {
		try {
			return NumberParser.getDouble(value, 0, value.length());
		} catch (NumberFormatException nfe) {
		}
		return NULL_DOUBLE;
	}

	/**
	 * Parses the given value as an int.
	 * 
	 * @param value
	 *            The string representation of the int value.
	 * @return The parsed value or {@link #NULL_INT} if the value cannot be parsed.
	 */
	public static int parseInt(final CharSequence value) {
		try {
			return NumberParser.getInteger(value);
		} catch (NumberFormatException nfe) {
		}
		return NULL_INT;
	}

	/**
	 * Parses the given value as a long.
	 * 
	 * @param value
	 *            The string representation of the long value.
	 * @return The parsed value or {@link #NULL_LONG} if the value cannot be parsed.
	 */
	public static long parseLong(final CharSequence value) {
		try {
			return NumberParser.getLong(value);
		} catch (NumberFormatException nfe) {
		}
		return NULL_LONG;
	}

}

final class NumberParser {

	private final static boolean USE_POW_TABLE = true;

	// Precompute Math.pow(10, n) as table:
	private final static int POW_RANGE = (USE_POW_TABLE) ? 256 : 0;
	private final static double[] POS_EXPS = new double[POW_RANGE];
	private final static double[] NEG_EXPS = new double[POW_RANGE];

	static {
		for (int i = 0; i < POW_RANGE; i++) {
			POS_EXPS[i] = FastMath.pow(10., i);
			NEG_EXPS[i] = FastMath.pow(10., -i);
		}
	}

	// Calculate the value of the specified exponent - reuse a precalculated value if possible
	private final static double getPow10(final int exp) {
		if (USE_POW_TABLE) {
			if (exp > -POW_RANGE) {
				if (exp <= 0)
					return NEG_EXPS[-exp];
				else if (exp < POW_RANGE)
					return POS_EXPS[exp];
			}
		}
		return Math.pow(10., exp);
	}

	public static int getInteger(final CharSequence csq) throws NumberFormatException {
		return getInteger(csq, 0, csq.length());
	}

	public static int getInteger(final CharSequence csq, final int offset, final int end) throws NumberFormatException {
		int off = offset;
		boolean sign = false;
		char ch;
		if ((end == 0) || (((ch = csq.charAt(off)) < '0') || (ch > '9'))
				&& (!(sign = ch == '-') || (++off == end) || (((ch = csq.charAt(off)) < '0') || (ch > '9'))))
			throw new NumberFormatException(csq.toString());
		// check overflow:
		final int limit = (sign) ? (-Integer.MAX_VALUE / 10) : (-Integer.MIN_VALUE / 10); // inline

		for (int ival = 0;; ival *= 10) {
			ival += '0' - ch; // negative
			if (++off == end)
				return sign ? ival : -ival;
			if (((ch = csq.charAt(off)) < '0') || (ch > '9'))
				throw new NumberFormatException(csq.toString());
			if (ival < limit)
				throw new NumberFormatException(csq.toString());
		}
	}

	public static long getLong(final CharSequence csq) throws NumberFormatException {
		return getLong(csq, 0, csq.length());
	}

	public static long getLong(final CharSequence csq, final int offset, final int end) throws NumberFormatException {
		int off = offset;
		boolean sign = false;
		char ch;
		if ((end == 0) || (((ch = csq.charAt(off)) < '0') || (ch > '9'))
				&& (!(sign = ch == '-') || (++off == end) || (((ch = csq.charAt(off)) < '0') || (ch > '9'))))
			throw new NumberFormatException(csq.toString());
		// check overflow:
		final long limit = (sign) ? (-Long.MAX_VALUE / 10l) : (-Long.MIN_VALUE / 10l); // inline
		for (long lval = 0l;; lval *= 10l) {
			lval += '0' - ch; // negative
			if (++off == end)
				return sign ? lval : -lval;
			if (((ch = csq.charAt(off)) < '0') || (ch > '9'))
				throw new NumberFormatException(csq.toString());
			if (lval < limit)
				throw new NumberFormatException(csq.toString());
		}
	}

	public static double getDouble(final CharSequence csq) throws NumberFormatException {
		return getDouble(csq, 0, csq.length());
	}

	public static double getDouble(final CharSequence csq, final int offset, final int end)
			throws NumberFormatException {
		int off = offset;
		int len = end - offset;
		if (len == 0)
			return Double.NaN;
		char ch;
		boolean numSign = true;
		ch = csq.charAt(off);
		if (ch == '+') {
			off++;
			len--;
		} else if (ch == '-') {
			numSign = false;
			off++;
			len--;
		}
		double number;
		// Look for the special csqings NaN, Inf
		if (csq.equals("nan") || csq.equals("NAN"))
			number = Double.NaN;
		// Look for the longer csqing first then try the shorter.
		else if (csq.equals("infinity") || csq.equals("INFINITY"))
			number = Double.POSITIVE_INFINITY;
		else if (csq.equals("inf") || csq.equals("INF"))
			number = Double.POSITIVE_INFINITY;
		else {
			boolean error = true;
			int startOffset = off;
			double dval;
			// TODO: check too many digits (overflow)
			for (dval = 0d; (len > 0) && ((ch = csq.charAt(off)) >= '0') && (ch <= '9');) {
				dval *= 10d;
				dval += ch - '0';
				off++;
				len--;
			}
			int numberLength = off - startOffset;
			number = dval;
			if (numberLength > 0)
				error = false;
			// Check for fractional values after decimal
			if ((len > 0) && (csq.charAt(off) == '.')) {
				off++;
				len--;
				startOffset = off;
				// TODO: check too many digits (overflow)
				for (dval = 0d; (len > 0) && ((ch = csq.charAt(off)) >= '0') && (ch <= '9');) {
					dval *= 10d;
					dval += ch - '0';
					off++;
					len--;
				}
				numberLength = off - startOffset;
				if (numberLength > 0) {
					// TODO: try factorizing pow10 with exponent below: only 1 long + operation
					number += getPow10(-numberLength) * dval;
					error = false;
				}
			}
			if (error)
				throw new NumberFormatException("Invalid Double : " + csq);
			// Look for an exponent
			if (len > 0) {
				// note: ignore any non-digit character at end:
				if ((ch = csq.charAt(off)) == 'e' || ch == 'E') {
					off++;
					len--;
					if (len > 0) {
						boolean expSign = true;
						ch = csq.charAt(off);
						if (ch == '+') {
							off++;
							len--;
						} else if (ch == '-') {
							expSign = false;
							off++;
							len--;
						}
						int exponent = 0;
						// note: ignore any non-digit character at end:
						for (exponent = 0; (len > 0) && ((ch = csq.charAt(off)) >= '0') && (ch <= '9');) {
							exponent *= 10;
							exponent += ch - '0';
							off++;
							len--;
						}
						// TODO: check exponent < 1024 (overflow)
						if (!expSign)
							exponent = -exponent;
						// For very small numbers we try to minimize effects of denormalization.
						if (exponent > -300)
							number *= getPow10(exponent);
						else
							number = 1.0E-300 * (number * getPow10(exponent + 300));
					}
				}
			}
			// check other characters:
			if (len > 0)
				throw new NumberFormatException("Invalid Double : " + csq);
		}
		return (numSign) ? number : -number;
	}
}

final class FileManager {

	public enum Mode {
		r, w, rw
	};

	private BufferedReader reader;
	private BufferedWriter writer;

	public FileManager(String filename, Mode mode) throws IOException {
		File file = new File(filename);
		boolean toRead = mode == Mode.rw || mode == Mode.r;
		boolean toWrite = mode == Mode.rw || mode == Mode.w;
		if (toRead)
			reader = new BufferedReader(new FileReader(file));
		if (toWrite)
			writer = new BufferedWriter(new FileWriter(file));
	}

	public BufferedReader getReader() {
		return reader;
	}

	public BufferedWriter getWriter() {
		return writer;
	}

	public void cleanup() {
		try {
			reader.close();
		} catch (Exception e) {
		}
		try {
			writer.close();
		} catch (Exception e) {
		}
	}
}
