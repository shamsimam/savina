package edu.rice.habanero.benchmarks.concsll;

/**
 * Source: http://www.cs.ucsb.edu/~franklin/20/assigns/prog2files/MySortedLinkedList.java
 * <p/>
 * Stores a sorted list of items that implement the interface Comparable. Provides an iterator to allow stepping through
 * the list in a loop
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class SortedLinkedList<T extends Comparable<T>> {

    /**
     * stores a single item in the linked list
     */
    private static class Node<T extends Comparable<T>> {
        public T item;
        public Node<T> next;

        public Node(final T i) {
            item = i;
            next = null;
        }
    }

    // a reference to the first node in the list
    private Node<T> head;
    // a reference to the node to return when next() is called
    private Node<T> iterator;

    /**
     * constructor creates a linked list with no items in it
     */
    protected SortedLinkedList() {
        head = null;
        iterator = null;
    }

    /**
     * isEmpty inputs: none return value: returns true if there are no items in linked list
     */
    public boolean isEmpty() {
        return (head == null);
    }

    /**
     * add inputs: Comparable item return value: none adds an item into the list in sorted order
     */
    public void add(final T item) {
        // make the new node to insert into list
        final Node<T> newNode = new Node<>(item);
        // first see if the list is empty
        if (head == null) {
            // System.out.println("add "+item +" to front");
            head = newNode;
        } else if (item.compareTo(head.item) < 0) {
            // there is something in the list
            // now check to see if it belongs in front
            // System.out.println("add "+item +"before"+head.item);
            newNode.next = head;
            head = newNode;
        } else {
            // otherwise, step down the list.  n will stop
            // at the node after the new node, and trailer will
            // stop at the node before the new node
            Node<T> after = head.next;
            Node<T> before = head;
            while (after != null) {
                if (item.compareTo(after.item) < 0) {
                    break;
                }
                before = after;
                after = after.next;
            }
            // insert between before & after
            newNode.next = before.next;
            before.next = newNode;
            // System.out.println("add " + item + "after" + before.item);
        }
    }

    /* contains
      * inputs: Comparable item
      * return value: true if equal item is in list, false otherwise
      */
    public boolean contains(final T item) {
        Node<T> n = head;
        // for each node in the linked list
        while (n != null) {
            // if it is equal, return true
            // note that I used compareTo here, not equals
            // because I am only guaranteed that the
            // compareTo method is implemented, not equals
            if (item.compareTo(n.item) == 0) {
                return true;
            }
            n = n.next;
        }
        // if it is not found in list, return false
        return false;
    }

    /**
     * toString inputs: none return value: string representation of the linked list items Format must match assignment
     */
    public String toString() {
        final StringBuilder s = new StringBuilder(100);
        Node<T> n = head;
        // for each node in the linked list
        while (n != null) {
            s.append(n.item.toString());
            n = n.next;
        }
        // if it is not found in list, return false
        return s.toString();
    }

    /**
     * next inputs: none return value: one element from the linked list This method returns each element in the linked
     * list in order. It is to be used in a loop to access every item in the list.
     */
    public Comparable<T> next() {
        if (iterator != null) {
            final Node<T> n = iterator;
            iterator = iterator.next;
            return n.item;
        } else {
            return null;
        }
    }

    /**
     * reset inputs: none return value: none resets the iterator so that the next call to next() will return the first
     * element in the list
     */
    public void reset() {
        iterator = head;
    }

    /**
     * size inputs: none return value: the number of elements in linked list
     */
    public int size() {
        int r = 0;
        Node<T> n = head;
        // for each node in the linked list
        while (n != null) {
            r++;
            n = n.next;
        }
        // if it is not found in list, return false
        return r;
    }
}
