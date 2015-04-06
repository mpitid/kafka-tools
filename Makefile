
.PHONY: clean distclean

c := consumer
p := producer
bin := bin

all: $(bin)/$c.sh $(bin)/$p.sh

$(bin)/%.sh: */target/%.jar
	$(bin)/jarpack $< $@

%.jar:
	mvn package -q -P shade

clean:
	mvn clean -q

distclean: clean
	$(RM) $(bin)/$c.sh $(bin)/$p.sh

