
.PHONY: clean distclean

cli := kafka8-tools
bin := bin

all: $(bin)/$(cli).sh

$(bin)/%.sh: target/%.jar
	$(bin)/jarpack $< $@

%.jar:
	mvn package -q -P shade

clean:
	mvn clean -q

distclean: clean
	$(RM) $(bin)/$(cli).sh

