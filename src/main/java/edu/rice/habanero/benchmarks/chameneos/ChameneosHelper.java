package edu.rice.habanero.benchmarks.chameneos;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public class ChameneosHelper {

    protected static Color complement(final Color color, final Color otherColor) {
        switch (color) {
            case RED:
                switch (otherColor) {
                    case RED:
                        return Color.RED;
                    case YELLOW:
                        return Color.BLUE;
                    case BLUE:
                        return Color.YELLOW;
                    case FADED:
                        return Color.FADED;
                }
                break;
            case YELLOW:
                switch (otherColor) {
                    case RED:
                        return Color.BLUE;
                    case YELLOW:
                        return Color.YELLOW;
                    case BLUE:
                        return Color.RED;
                    case FADED:
                        return Color.FADED;
                }
                break;
            case BLUE:
                switch (otherColor) {
                    case RED:
                        return Color.YELLOW;
                    case YELLOW:
                        return Color.RED;
                    case BLUE:
                        return Color.BLUE;
                    case FADED:
                        return Color.FADED;
                }
                break;
            case FADED:
                return Color.FADED;

        }
        throw new IllegalArgumentException("Unknown color: " + color);
    }

    protected static Color fadedColor() {
        return Color.FADED;
    }

    enum Color {
        RED,
        YELLOW,
        BLUE,
        FADED
    }

    protected static interface Message {
    }

    protected static class MeetMsg implements Message {
        public final Color color;
        public final Object sender;

        protected MeetMsg(final Color color, final Object sender) {
            this.color = color;
            this.sender = sender;
        }
    }

    protected static class ChangeMsg implements Message {
        public final Color color;
        public final Object sender;

        protected ChangeMsg(final Color color, final Object sender) {
            this.color = color;
            this.sender = sender;
        }
    }

    protected static class MeetingCountMsg implements Message {
        public final int count;
        public final Object sender;

        protected MeetingCountMsg(final int count, final Object sender) {
            this.count = count;
            this.sender = sender;
        }
    }

    protected static class ExitMsg implements Message {
        public final Object sender;

        protected ExitMsg(final Object sender) {
            this.sender = sender;
        }
    }

}
