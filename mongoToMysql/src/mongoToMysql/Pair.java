package mongoToMysql;

public class Pair<E, F> {
	 
	public E first;
	public F second;
 
	/**
	 * Constructor
	 * 
	 * @param first  The first element of the new pair
	 * @param second The second element of the new pair
	 */
	public Pair(E first, F second) {
		this.first = first;
		this.second = second;
	}
 
	/**
	 * The first element of this pair
	 */
	public E getFirst() {
		return first;
	}
 
	/**
	 * The second element of this pair
	 */
	public F getSecond() {
		return second;
	}
 
	/**
	 * Textual representation
	 */
	public String toString() {
		return "(" + getFirst() + ", " + getSecond() + ") ";
	}
}
