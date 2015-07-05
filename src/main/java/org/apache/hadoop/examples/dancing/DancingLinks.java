package org.apache.hadoop.examples.dancing;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DancingLinks<ColumnName> {
	private static final Log LOG = LogFactory.getLog(DancingLinks.class
			.getName());
	private ColumnHeader<ColumnName> head;
	private List<ColumnHeader<ColumnName>> columns;

	public DancingLinks() {
		this.head = new ColumnHeader(null, 0);
		this.head.left = this.head;
		this.head.right = this.head;
		this.head.up = this.head;
		this.head.down = this.head;
		this.columns = new ArrayList(200);
	}

	public void addColumn(ColumnName name, boolean primary) {
		ColumnHeader top = new ColumnHeader(name, 0);
		top.up = top;
		top.down = top;
		if (primary) {
			Node tail = this.head.left;
			tail.right = top;
			top.left = tail;
			top.right = this.head;
			this.head.left = top;
		} else {
			top.left = top;
			top.right = top;
		}
		this.columns.add(top);
	}

	public void addColumn(ColumnName name) {
		addColumn(name, true);
	}

	public int getNumberColumns() {
		return this.columns.size();
	}

	public String getColumnName(int index) {
		return ((ColumnHeader) this.columns.get(index)).name.toString();
	}

	public void addRow(boolean[] values) {
		Node prev = null;
		for (int i = 0; i < values.length; i++)
			// if (values[i] != 0) {
			if (values[i] != false) {
				ColumnHeader top = (ColumnHeader) this.columns.get(i);
				top.size += 1;
				Node bottom = top.up;
				Node node = new Node(null, null, bottom, top, top);

				bottom.down = node;
				top.up = node;
				if (prev != null) {
					Node front = prev.right;
					node.left = prev;
					node.right = front;
					prev.right = node;
					front.left = node;
				} else {
					node.left = node;
					node.right = node;
				}
				prev = node;
			}
	}

	private ColumnHeader<ColumnName> findBestColumn() {
		int lowSize = 2147483647;
		ColumnHeader result = null;
		ColumnHeader current = (ColumnHeader) this.head.right;
		while (current != this.head) {
			if (current.size < lowSize) {
				lowSize = current.size;
				result = current;
			}
			current = (ColumnHeader) current.right;
		}
		return result;
	}

	private void coverColumn(ColumnHeader<ColumnName> col) {
		LOG.debug("cover " + col.head.name);

		col.right.left = col.left;
		col.left.right = col.right;
		Node row = col.down;
		while (row != col) {
			Node node = row.right;
			while (node != row) {
				node.down.up = node.up;
				node.up.down = node.down;
				node.head.size -= 1;
				node = node.right;
			}
			row = row.down;
		}
	}

	private void uncoverColumn(ColumnHeader<ColumnName> col) {
		LOG.debug("uncover " + col.head.name);
		Node row = col.up;
		while (row != col) {
			Node node = row.left;
			while (node != row) {
				node.head.size += 1;
				node.down.up = node;
				node.up.down = node;
				node = node.left;
			}
			row = row.up;
		}
		col.right.left = col;
		col.left.right = col;
	}

	private List<ColumnName> getRowName(Node<ColumnName> row) {
		List result = new ArrayList();
		result.add(row.head.name);
		Node node = row.right;
		while (node != row) {
			result.add(node.head.name);
			node = node.right;
		}
		return result;
	}

	private int search(List<Node<ColumnName>> partial,
			SolutionAcceptor<ColumnName> output) {
		int results = 0;
		if (this.head.right == this.head) {
			List result = new ArrayList(partial.size());
			for (Node row : partial) {
				result.add(getRowName(row));
			}
			output.solution(result);
			results++;
		} else {
			ColumnHeader col = findBestColumn();
			if (col.size > 0) {
				coverColumn(col);
				Node row = col.down;
				while (row != col) {
					partial.add(row);
					Node node = row.right;
					while (node != row) {
						coverColumn(node.head);
						node = node.right;
					}
					results += search(partial, output);
					partial.remove(partial.size() - 1);
					node = row.left;
					while (node != row) {
						uncoverColumn(node.head);
						node = node.left;
					}
					row = row.down;
				}
				uncoverColumn(col);
			}
		}
		return results;
	}

	private void searchPrefixes(int depth, int[] choices, List<int[]> prefixes) {
		if (depth == 0) {
			prefixes.add(choices.clone());
		} else {
			ColumnHeader col = findBestColumn();
			if (col.size > 0) {
				coverColumn(col);
				Node row = col.down;
				int rowId = 0;
				while (row != col) {
					Node node = row.right;
					while (node != row) {
						coverColumn(node.head);
						node = node.right;
					}
					choices[(choices.length - depth)] = rowId;
					searchPrefixes(depth - 1, choices, prefixes);
					node = row.left;
					while (node != row) {
						uncoverColumn(node.head);
						node = node.left;
					}
					row = row.down;
					rowId++;
				}
				uncoverColumn(col);
			}
		}
	}

	public List<int[]> split(int depth) {
		int[] choices = new int[depth];
		List result = new ArrayList(100000);
		searchPrefixes(depth, choices, result);
		return result;
	}

	private Node<ColumnName> advance(int goalRow) {
		ColumnHeader col = findBestColumn();
		if (col.size > 0) {
			coverColumn(col);
			Node row = col.down;
			int id = 0;
			while (row != col) {
				if (id == goalRow) {
					Node node = row.right;
					while (node != row) {
						coverColumn(node.head);
						node = node.right;
					}
					return row;
				}
				id++;
				row = row.down;
			}
		}
		return null;
	}

	private void rollback(Node<ColumnName> row) {
		Node node = row.left;
		while (node != row) {
			uncoverColumn(node.head);
			node = node.left;
		}
		uncoverColumn(row.head);
	}

	public int solve(int[] prefix, SolutionAcceptor<ColumnName> output) {
		List choices = new ArrayList();
		for (int i = 0; i < prefix.length; i++) {
			choices.add(advance(prefix[i]));
		}
		int result = search(choices, output);
		for (int i = prefix.length - 1; i >= 0; i--) {
			rollback((Node) choices.get(i));
		}
		return result;
	}

	public int solve(SolutionAcceptor<ColumnName> output) {
		return search(new ArrayList(), output);
	}

	public static abstract interface SolutionAcceptor<ColumnName> {
		public abstract void solution(List<List<ColumnName>> paramList);
	}

	private static class ColumnHeader<ColumnName> extends
			DancingLinks.Node<ColumnName> {
		ColumnName name;
		int size;

		ColumnHeader(ColumnName n, int s) {
			this.name = n;
			this.size = s;
			this.head = this;
		}

		ColumnHeader() {
			this(null, 0);
		}
	}

	private static class Node<ColumnName> {
		Node<ColumnName> left;
		Node<ColumnName> right;
		Node<ColumnName> up;
		Node<ColumnName> down;
		DancingLinks.ColumnHeader<ColumnName> head;

		Node(Node<ColumnName> l, Node<ColumnName> r, Node<ColumnName> u,
				Node<ColumnName> d, DancingLinks.ColumnHeader<ColumnName> h) {
			this.left = l;
			this.right = r;
			this.up = u;
			this.down = d;
			this.head = h;
		}

		Node() {
			this(null, null, null, null, null);
		}
	}
}
