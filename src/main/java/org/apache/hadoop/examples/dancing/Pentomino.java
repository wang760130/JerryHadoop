package org.apache.hadoop.examples.dancing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class Pentomino {
	protected int width;
	protected int height;
	protected List<Piece> pieces = new ArrayList();

	protected static final int[] oneRotation = { 0 };

	protected static final int[] twoRotations = { 0, 1 };

	protected static final int[] fourRotations = { 0, 1, 2, 3 };

	private DancingLinks<ColumnName> dancer = new DancingLinks();
	private DancingLinks.SolutionAcceptor<ColumnName> printer;

	public static String stringifySolution(int width, int height,
			List<List<ColumnName>> solution) {
		String[][] picture = new String[height][width];
		StringBuffer result = new StringBuffer();

		for (List<ColumnName> row : solution) {
			Piece piece = null;
			for (ColumnName item : row) {
				if ((item instanceof Piece)) {
					piece = (Piece) item;
					break;
				}
			}

			for (ColumnName item : row)
				if ((item instanceof Point)) {
					Point p = (Point) item;
					picture[p.y][p.x] = piece.getName();
				}
		}
		Piece piece;
		for (int y = 0; y < picture.length; y++) {
			for (int x = 0; x < picture[y].length; x++) {
				result.append(picture[y][x]);
			}
			result.append("\n");
		}
		return result.toString();
	}

	public SolutionCategory getCategory(List<List<ColumnName>> names) {
		Piece xPiece = null;

		for (Piece p : this.pieces) {
			if ("x".equals(p.name)) {
				xPiece = p;
				break;
			}
		}

		for (List<ColumnName> row : names) {
			if (row.contains(xPiece)) {
				int low_x = this.width;
				int high_x = 0;
				int low_y = this.height;
				int high_y = 0;
				for (ColumnName col : row) {
					if ((col instanceof Point)) {
						int x = ((Point) col).x;
						int y = ((Point) col).y;
						if (x < low_x) {
							low_x = x;
						}
						if (x > high_x) {
							high_x = x;
						}
						if (y < low_y) {
							low_y = y;
						}
						if (y > high_y) {
							high_y = y;
						}
					}
				}
				boolean mid_x = low_x + high_x == this.width - 1;
				boolean mid_y = low_y + high_y == this.height - 1;
				if ((mid_x) && (mid_y))
					return SolutionCategory.CENTER;
				if (mid_x)
					return SolutionCategory.MID_X;
				if (!mid_y)
					break;
				return SolutionCategory.MID_Y;
			}

		}

		return SolutionCategory.UPPER_LEFT;
	}

	protected void initializePieces() {
		this.pieces.add(new Piece("x", " x /xxx/ x ", false, oneRotation));
		this.pieces.add(new Piece("v", "x  /x  /xxx", false, fourRotations));
		this.pieces.add(new Piece("t", "xxx/ x / x ", false, fourRotations));
		this.pieces.add(new Piece("w", "  x/ xx/xx ", false, fourRotations));
		this.pieces.add(new Piece("u", "x x/xxx", false, fourRotations));
		this.pieces.add(new Piece("i", "xxxxx", false, twoRotations));
		this.pieces.add(new Piece("f", " xx/xx / x ", true, fourRotations));
		this.pieces.add(new Piece("p", "xx/xx/x ", true, fourRotations));
		this.pieces.add(new Piece("z", "xx / x / xx", true, twoRotations));
		this.pieces.add(new Piece("n", "xx  / xxx", true, fourRotations));
		this.pieces.add(new Piece("y", "  x /xxxx", true, fourRotations));
		this.pieces.add(new Piece("l", "   x/xxxx", true, fourRotations));
	}

	private static boolean isSide(int offset, int shapeSize, int board) {
		return 2 * offset + shapeSize <= board;
	}

	private static void generateRows(DancingLinks dancer, Piece piece,
			int width, int height, boolean flip, boolean[] row,
			boolean upperLeft) {
		int[] rotations = piece.getRotations();
		for (int rotIndex = 0; rotIndex < rotations.length; rotIndex++) {
			boolean[][] shape = piece.getShape(flip, rotations[rotIndex]);

			for (int x = 0; x < width; x++)
				for (int y = 0; y < height; y++)
					if ((y + shape.length <= height)
							&& (x + shape[0].length <= width)
							&& ((!upperLeft) || ((isSide(x, shape[0].length,
									width)) && (isSide(y, shape.length, height))))) {
						for (int idx = 0; idx < width * height; idx++) {
							row[idx] = false;
						}

						for (int subY = 0; subY < shape.length; subY++) {
							for (int subX = 0; subX < shape[0].length; subX++) {
								row[((y + subY) * width + x + subX)] = shape[subY][subX];
							}
						}
						dancer.addRow(row);
					}
		}
	}

	public Pentomino(int width, int height) {
		initializePieces();

		initialize(width, height);
	}

	public Pentomino() {
		initializePieces();
	}

	void initialize(int width, int height) {
		this.width = width;
		this.height = height;
		for (int y = 0; y < height; y++) {
			for (int x = 0; x < width; x++) {
				this.dancer.addColumn(new Point(x, y));
			}
		}
		int pieceBase = this.dancer.getNumberColumns();
		for (Piece p : this.pieces) {
			this.dancer.addColumn(p);
		}
		boolean[] row = new boolean[this.dancer.getNumberColumns()];
		for (int idx = 0; idx < this.pieces.size(); idx++) {
			Piece piece = (Piece) this.pieces.get(idx);
			row[(idx + pieceBase)] = true;
			generateRows(this.dancer, piece, width, height, false, row,
					idx == 0);
			if (piece.getFlippable()) {
				generateRows(this.dancer, piece, width, height, true, row,
						idx == 0);
			}
			row[(idx + pieceBase)] = false;
		}
		this.printer = new SolutionPrinter(width, height);
	}

	public List<int[]> getSplits(int depth) {
		return this.dancer.split(depth);
	}

	public int solve(int[] split) {
		return this.dancer.solve(split, this.printer);
	}

	public int solve() {
		return this.dancer.solve(this.printer);
	}

	public void setPrinter(DancingLinks.SolutionAcceptor<ColumnName> printer) {
		this.printer = printer;
	}

	public static void main(String[] args) {
		int width = 6;
		int height = 10;
		Pentomino model = new Pentomino(width, height);
		List splits = model.getSplits(2);
		for (Iterator splitItr = splits.iterator(); splitItr.hasNext();) {
			int[] choices = (int[]) splitItr.next();
			System.out.print("split:");
			for (int i = 0; i < choices.length; i++) {
				System.out.print(" " + choices[i]);
			}
			System.out.println();

			System.out.println(model.solve(choices) + " solutions found.");
		}
	}

	private static class SolutionPrinter implements
			DancingLinks.SolutionAcceptor<Pentomino.ColumnName> {
		int width;
		int height;

		public SolutionPrinter(int width, int height) {
			this.width = width;
			this.height = height;
		}

		public void solution(List<List<Pentomino.ColumnName>> names) {
			System.out.println(Pentomino.stringifySolution(this.width,
					this.height, names));
		}
	}

	public static enum SolutionCategory {
		UPPER_LEFT, MID_X, MID_Y, CENTER;
	}

	static class Point implements Pentomino.ColumnName {
		int x;
		int y;

		Point(int x, int y) {
			this.x = x;
			this.y = y;
		}
	}

	protected static class Piece implements Pentomino.ColumnName {
		private String name;
		private boolean[][] shape;
		private int[] rotations;
		private boolean flippable;

		public Piece(String name, String shape, boolean flippable,
				int[] rotations) {
			this.name = name;
			this.rotations = rotations;
			this.flippable = flippable;
			StringTokenizer parser = new StringTokenizer(shape, "/");
			List lines = new ArrayList();
			while (parser.hasMoreTokens()) {
				String token = parser.nextToken();
				boolean[] line = new boolean[token.length()];
				for (int i = 0; i < line.length; i++) {
					line[i] = (token.charAt(i) == 'x' ? true : false);
				}
				lines.add(line);
			}
			this.shape = new boolean[lines.size()][];
			for (int i = 0; i < lines.size(); i++)
				this.shape[i] = ((boolean[]) (boolean[]) lines.get(i));
		}

		public String getName() {
			return this.name;
		}

		public int[] getRotations() {
			return this.rotations;
		}

		public boolean getFlippable() {
			return this.flippable;
		}

		private int doFlip(boolean flip, int x, int max) {
			if (flip) {
				return max - x - 1;
			}
			return x;
		}

		public boolean[][] getShape(boolean flip, int rotate) {
			boolean[][] result;
			if (rotate % 2 == 0) {
				int height = this.shape.length;
				int width = this.shape[0].length;
				result = new boolean[height][];
				boolean flipX = rotate == 2;
				boolean flipY = flip ^ rotate == 2;
				for (int y = 0; y < height; y++) {
					result[y] = new boolean[width];
					for (int x = 0; x < width; x++)
						result[y][x] = this.shape[doFlip(flipY, y, height)][doFlip(
								flipX, x, width)];
				}
			} else {
				int height = this.shape[0].length;
				int width = this.shape.length;
				result = new boolean[height][];
				boolean flipX = rotate == 3;
				boolean flipY = flip ^ rotate == 1;
				for (int y = 0; y < height; y++) {
					result[y] = new boolean[width];
					for (int x = 0; x < width; x++) {
						result[y][x] = this.shape[doFlip(flipX, x, width)][doFlip(
								flipY, y, height)];
					}
				}
			}

			return result;
		}
	}

	protected static abstract interface ColumnName {
	}
}
