package org.apache.hadoop.examples.dancing;

public class OneSidedPentomino extends Pentomino {
	public OneSidedPentomino() {
	}

	public OneSidedPentomino(int width, int height) {
		super(width, height);
	}

	protected void initializePieces() {
		this.pieces.add(new Pentomino.Piece("x", " x /xxx/ x ", false,
				oneRotation));
		this.pieces.add(new Pentomino.Piece("v", "x  /x  /xxx", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("t", "xxx/ x / x ", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("w", "  x/ xx/xx ", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("u", "x x/xxx", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("i", "xxxxx", false, twoRotations));
		this.pieces.add(new Pentomino.Piece("f", " xx/xx / x ", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("p", "xx/xx/x ", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("z", "xx / x / xx", false,
				twoRotations));
		this.pieces.add(new Pentomino.Piece("n", "xx  / xxx", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("y", "  x /xxxx", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("l", "   x/xxxx", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("F", "xx / xx/ x ", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("P", "xx/xx/ x", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("Z", " xx/ x /xx ", false,
				twoRotations));
		this.pieces.add(new Pentomino.Piece("N", "  xx/xxx ", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("Y", " x  /xxxx", false,
				fourRotations));
		this.pieces.add(new Pentomino.Piece("L", "x   /xxxx", false,
				fourRotations));
	}

	public static void main(String[] args) {
		Pentomino model = new OneSidedPentomino(3, 30);
		int solutions = model.solve();
		System.out.println(solutions + " solutions found.");
	}
}
