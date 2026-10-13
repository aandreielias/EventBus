package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChannelTest {

    @Test
    void testChannelCreationAndEquality() {
        Channel c1 = Channel.of("ui", "button", "click");
        Channel c2 = Channel.parse("ui/button/click");

        assertThat(c1).isEqualTo(c2);
        assertThat(c1.hashCode()).isEqualTo(c2.hashCode());
        assertThat(c1.getDepth()).isEqualTo(3);
        assertThat(c1.isRoot()).isFalse();
    }

    @Test
    void testSegmentTrimmingConsistency() {
        Channel c1 = Channel.of("ui", " button ", "click ");
        Channel c2 = Channel.parse("ui/button/click");

        assertThat(c1).isEqualTo(c2);
        assertThat(c1.toString()).isEqualTo("ui/button/click");
    }

    @Test
    void testParentAndAncestry() {
        Channel root = Channel.of("ui");
        Channel parent = Channel.parse("ui/button");
        Channel child = Channel.parse("ui/button/click");

        assertThat(root.isRoot()).isTrue();
        assertThat(root.getParent()).isEmpty();

        assertThat(child.getParent()).contains(parent);
        assertThat(parent.getParent()).contains(root);

        assertThat(root.isParentOf(child)).isTrue();
        assertThat(parent.isParentOf(child)).isTrue();
        assertThat(child.isChildOf(root)).isTrue();
        assertThat(child.isChildOf(parent)).isTrue();
        assertThat(child.isParentOf(root)).isFalse();
    }

    @Test
    void testCustomSeparator() {
        Channel c = Channel.parse("finance.payment.process", ".");
        assertThat(c.getSegments()).containsExactly("finance", "payment", "process");
        assertThat(c.toString(".")).isEqualTo("finance.payment.process");
    }

    @Test
    void testInvalidSegments() {
        assertThatThrownBy(() -> Channel.of())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Channel.of(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Channel.parse(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Channel.parse("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
