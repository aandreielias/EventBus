package ae.eventbus.cli;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal left-aligned text table for terminal output.
 */
final class TextTable {

    private static final String GAP = "  ";

    private final List<String> headers;
    private final List<List<String>> rows = new ArrayList<>();

    TextTable(String... headers) {
        this.headers = List.of(headers);
    }

    /** Adds a row; {@code null} cells are shown as {@code -}. */
    TextTable row(Object... cells) {
        if (cells.length != headers.size()) {
            throw new IllegalArgumentException("Expected " + headers.size() + " cells, got " + cells.length);
        }
        List<String> row = new ArrayList<>(cells.length);
        for (Object cell : cells) {
            row.add(cell == null ? "-" : cell.toString());
        }
        rows.add(row);
        return this;
    }

    void print(PrintStream out) {
        if (rows.isEmpty()) {
            out.println("(none)");
            return;
        }
        int[] widths = new int[headers.size()];
        for (int c = 0; c < widths.length; c++) {
            widths[c] = headers.get(c).length();
            for (List<String> row : rows) {
                widths[c] = Math.max(widths[c], row.get(c).length());
            }
        }
        printRow(out, headers, widths);
        for (List<String> row : rows) {
            printRow(out, row, widths);
        }
    }

    private static void printRow(PrintStream out, List<String> cells, int[] widths) {
        StringBuilder line = new StringBuilder();
        for (int c = 0; c < cells.size(); c++) {
            boolean last = c == cells.size() - 1;
            line.append(last ? cells.get(c) : String.format("%-" + widths[c] + "s", cells.get(c)));
            if (!last) line.append(GAP);
        }
        out.println(line);
    }
}
