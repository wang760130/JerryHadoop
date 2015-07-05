package org.apache.hadoop.examples.dancing;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class Sudoku {
	private int[][] board;
	private int size;
	private int squareXSize;
	private int squareYSize;

	static String stringifySolution(int size, List<List<ColumnName>> solution) {
		int[][] picture = new int[size][size];
		StringBuffer result = new StringBuffer();

		for (List<ColumnName> row : solution) {
			int x = -1;
			int y = -1;
			int num = -1;
			for (ColumnName item : row) {
				if ((item instanceof ColumnConstraint)) {
					x = ((ColumnConstraint) item).column;
					num = ((ColumnConstraint) item).num;
				} else if ((item instanceof RowConstraint)) {
					y = ((RowConstraint) item).row;
				}
			}
			picture[y][x] = num;
		}

		for (int y = 0; y < size; y++) {
			for (int x = 0; x < size; x++) {
				result.append(picture[y][x]);
				result.append(" ");
			}
			result.append("\n");
		}
		return result.toString();
	}

	public Sudoku(InputStream stream) throws IOException {
		BufferedReader file = new BufferedReader(new InputStreamReader(stream));
		String line = file.readLine();
		List result = new ArrayList();
		while (line != null) {
			StringTokenizer tokenizer = new StringTokenizer(line);
			int size = tokenizer.countTokens();
			int[] col = new int[size];
			int y = 0;
			while (tokenizer.hasMoreElements()) {
				String word = tokenizer.nextToken();
				if ("?".equals(word))
					col[y] = -1;
				else {
					col[y] = Integer.parseInt(word);
				}
				y++;
			}
			result.add(col);
			line = file.readLine();
		}
		this.size = result.size();
		this.board = ((int[][]) result.toArray(new int[this.size][]));
		this.squareYSize = ((int) Math.sqrt(this.size));
		this.squareXSize = (this.size / this.squareYSize);
		file.close();
	}

	private boolean[] generateRow(boolean[] rowValues, int x, int y, int num) {
		for (int i = 0; i < rowValues.length; i++) {
			rowValues[i] = false;
		}

		int xBox = x / this.squareXSize;
		int yBox = y / this.squareYSize;

		rowValues[(x * this.size + num - 1)] = true;

		rowValues[(this.size * this.size + y * this.size + num - 1)] = true;

		rowValues[(2 * this.size * this.size + (xBox * this.squareXSize + yBox)
				* this.size + num - 1)] = true;

		rowValues[(3 * this.size * this.size + this.size * x + y)] = true;
		return rowValues;
	}

	private DancingLinks<ColumnName> makeModel() {
		DancingLinks model = new DancingLinks();

		for (int x = 0; x < this.size; x++) {
			for (int num = 1; num <= this.size; num++) {
				model.addColumn(new ColumnConstraint(num, x));
			}
		}

		for (int y = 0; y < this.size; y++) {
			for (int num = 1; num <= this.size; num++) {
				model.addColumn(new RowConstraint(num, y));
			}
		}

		for (int x = 0; x < this.squareYSize; x++) {
			for (int y = 0; y < this.squareXSize; y++) {
				for (int num = 1; num <= this.size; num++) {
					model.addColumn(new SquareConstraint(num, x, y));
				}
			}
		}

		for (int x = 0; x < this.size; x++) {
			for (int y = 0; y < this.size; y++) {
				model.addColumn(new CellConstraint(x, y));
			}
		}
		boolean[] rowValues = new boolean[this.size * this.size * 4];
		for (int x = 0; x < this.size; x++) {
			for (int y = 0; y < this.size; y++) {
				if (this.board[y][x] == -1) {
					for (int num = 1; num <= this.size; num++) {
						model.addRow(generateRow(rowValues, x, y, num));
					}
				} else {
					model.addRow(generateRow(rowValues, x, y, this.board[y][x]));
				}
			}
		}
		return model;
	}

	public void solve() {
		DancingLinks model = makeModel();
		int results = model.solve(new SolutionPrinter(this.size));
		System.out.println("Found " + results + " solutions");
	}

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("Include a puzzle on the command line.");
		}
		for (int i = 0; i < args.length; i++) {
			Sudoku problem = new Sudoku(new FileInputStream(args[i]));
			System.out.println("Solving " + args[i]);
			problem.solve();
		}
	}

	private static class CellConstraint implements Sudoku.ColumnName {
		int x;
		int y;

		CellConstraint(int x, int y) {
			this.x = x;
			this.y = y;
		}

		public String toString() {
			return "cell " + this.x + "," + this.y;
		}
	}

	private static class SquareConstraint implements Sudoku.ColumnName {
		int num;
		int x;
		int y;

		SquareConstraint(int num, int x, int y) {
			this.num = num;
			this.x = x;
			this.y = y;
		}

		public String toString() {
			return this.num + " in square " + this.x + "," + this.y;
		}
	}

	private static class RowConstraint implements Sudoku.ColumnName {
		int num;
		int row;

		RowConstraint(int num, int row) {
			this.num = num;
			this.row = row;
		}

		public String toString() {
			return this.num + " in row " + this.row;
		}
	}

	private static class ColumnConstraint implements Sudoku.ColumnName {
		int num;
		int column;

		ColumnConstraint(int num, int column) {
			this.num = num;
			this.column = column;
		}

		public String toString() {
			return this.num + " in column " + this.column;
		}
	}

	private static class SolutionPrinter implements
			DancingLinks.SolutionAcceptor<Sudoku.ColumnName> {
		int size;

		public SolutionPrinter(int size) {
			this.size = size;
		}

		void rawWrite(List solution) {
			for (Iterator itr = solution.iterator(); itr.hasNext();) {
				Iterator subitr = ((List) itr.next()).iterator();
				while (subitr.hasNext()) {
					System.out.print(subitr.next().toString() + " ");
				}
				System.out.println();
			}
		}

		public void solution(List<List<Sudoku.ColumnName>> names) {
			System.out.println(Sudoku.stringifySolution(this.size, names));
		}
	}

	protected static abstract interface ColumnName {
	}
}