/*
 *  (c) Copyright 2001, 2003, 2004, 2005, 2006, 2007, 2008, 2009 Hewlett-Packard Development Company, LP
 *  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.

 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
package pigUtils.ntriples;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Pig loader for NTriples formatted RDF files assuming that subject, predicate
 * are valid URIs (e.g. clear of any whitespace character) and that the object
 * is Literal.
 * 
 * Returns the subject and the object (parsed as a String) and the language code
 * if any (or null). The property value can be given as a filter.
 * 
 * The NTriples literal parsing is taken from the Jena project (which
 * unfortunately does not offer a pure String API that does not allocate Jena
 * specific java objects.
 */
public class nTriplesLoader extends AbstractNTriplesLoader {

    public nTriplesLoader() {
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        Schema schema = new Schema();
        schema.add(new FieldSchema("subject", DataType.CHARARRAY));
        schema.add(new FieldSchema("property", DataType.CHARARRAY));
        schema.add(new FieldSchema("object", DataType.CHARARRAY));
        schema.add(new FieldSchema("obj_is_uri", DataType.INTEGER));
        return new ResourceSchema(schema);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            String line = null;
            while (reader.nextKeyValue()) {
                line = reader.getCurrentValue().toString();
                String[] split = line.split(" ", 3);
                if (split.length != 3) {
                    // unexpected spaces, the object must be a literal, skip
                    continue;
                }
                if (split[2].startsWith("<")) {
                return tupleFactory.newTupleNoCopy(
                       Arrays.asList(
                         stripBrackets(split[0], null), 
                         stripBrackets(split[1], null),
                         split[2].substring(1, split[2].length()-3),
                         1
                         )
                       );
                } else {
                return tupleFactory.newTupleNoCopy(
                       Arrays.asList(
                         stripBrackets(split[0], null),
                         stripBrackets(split[1], null),
                         unescape_literal_string(split[2].substring(1, split[2].length()-3)),
                         0
                         )
                       );
                }
            }
            if (line != null) {
                // we reached the end of the file split with an non matching
                // line: return the (null, null) tuple instead of just null
                // since pig was not expecting this split to be fully consumed
                // by introspecting the InputFormat progress data.
                return tupleFactory.newTupleNoCopy(Arrays.asList(null, null, null));
            }
            return null;
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            throw new IOException(e);
        }
    }

/* "Borrowed" from 
   stackoverflow.com/questions/3537706/howto-unescape-a-java-string-literal-in-java
*/

  private final static
  String unescape_literal_string(String oldstr) {

    /*
     * In contrast to fixing Java's broken regex charclasses,
     * this one need be no bigger, as unescaping shrinks the string
     * here, where in the other one, it grows it.
     */

    StringBuffer newstr = new StringBuffer(oldstr.length());

    boolean saw_backslash = false;

    for (int i = 0; i < oldstr.length(); i++) {
        int cp = oldstr.codePointAt(i);
        if (oldstr.codePointAt(i) > Character.MAX_VALUE) {
            i++; /****WE HATES UTF-16! WE HATES IT FOREVERSES!!!****/
        }

        if (!saw_backslash) {
            if (cp == '\\') {
                saw_backslash = true;
            } else {
                newstr.append(Character.toChars(cp));
            }
            continue; /* switch */
        }

        if (cp == '\\') {
            saw_backslash = false;
            newstr.append('\\');
            newstr.append('\\');
            continue; /* switch */
        }

        switch (cp) {

            case 'r':  newstr.append('\r');
                       break; /* switch */

            case 'n':  newstr.append('\n');
                       break; /* switch */

            case 'f':  newstr.append('\f');
                       break; /* switch */

            /* PASS a \b THROUGH!! */
            case 'b':  newstr.append("\\b");
                       break; /* switch */

            case 't':  newstr.append('\t');
                       break; /* switch */

            case 'a':  newstr.append('\007');
                       break; /* switch */

            case 'e':  newstr.append('\033');
                       break; /* switch */

            /*
             * A "control" character is what you get when you xor its
             * codepoint with '@'==64.  This only makes sense for ASCII,
             * and may not yield a "control" character after all.
             *
             * Strange but true: "\c{" is ";", "\c}" is "=", etc.
             */
            case 'c':   {
                if (++i == oldstr.length()) { die("trailing \\c"); }
                cp = oldstr.codePointAt(i);
                /*
                 * don't need to grok surrogates, as next line blows them up
                 */
                if (cp > 0x7f) { die("expected ASCII after \\c"); }
                newstr.append(Character.toChars(cp ^ 64));
                break; /* switch */
            }

            case '8':
            case '9': die("illegal octal digit");
                      /* NOTREACHED */

    /*
     * may be 0 to 2 octal digits following this one
     * so back up one for fallthrough to next case;
     * unread this digit and fall through to next case.
     */
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7': --i;
                      /* FALLTHROUGH */

            /*
             * Can have 0, 1, or 2 octal digits following a 0
             * this permits larger values than octal 377, up to
             * octal 777.
             */
            case '0': {
                if (i+1 == oldstr.length()) {
                    /* found \0 at end of string */
                    newstr.append(Character.toChars(0));
                    break; /* switch */
                }
                i++;
                int digits = 0;
                int j;
                for (j = 0; j <= 2; j++) {
                    if (i+j == oldstr.length()) {
                        break; /* for */
                    }
                    /* safe because will unread surrogate */
                    int ch = oldstr.charAt(i+j);
                    if (ch < '0' || ch > '7') {
                        break; /* for */
                    }
                    digits++;
                }
                if (digits == 0) {
                    --i;
                    newstr.append('\0');
                    break; /* switch */
                }
                int value = 0;
                try {
                    value = Integer.parseInt(
                                oldstr.substring(i, i+digits), 8);
                } catch (NumberFormatException nfe) {
                    die("invalid octal value for \\0 escape");
                }
                newstr.append(Character.toChars(value));
                i += digits-1;
                break; /* switch */
            } /* end case '0' */

            case 'x':  {
                if (i+2 > oldstr.length()) {
                    die("string too short for \\x escape");
                }
                i++;
                boolean saw_brace = false;
                if (oldstr.charAt(i) == '{') {
                        /* ^^^^^^ ok to ignore surrogates here */
                    i++;
                    saw_brace = true;
                }
                int j;
                for (j = 0; j < 8; j++) {

                    if (!saw_brace && j == 2) {
                        break;  /* for */
                    }

                    /*
                     * ASCII test also catches surrogates
                     */
                    int ch = oldstr.charAt(i+j);
                    if (ch > 127) {
                        die("illegal non-ASCII hex digit in \\x escape");
                    }

                    if (saw_brace && ch == '}') { break; /* for */ }

                    if (! ( (ch >= '0' && ch <= '9')
                                ||
                            (ch >= 'a' && ch <= 'f')
                                ||
                            (ch >= 'A' && ch <= 'F')
                          )
                       )
                    {
                        die(String.format(
                            "illegal hex digit #%d '%c' in \\x", ch, ch));
                    }

                }
                if (j == 0) { die("empty braces in \\x{} escape"); }
                int value = 0;
                try {
                    value = Integer.parseInt(oldstr.substring(i, i+j), 16);
                } catch (NumberFormatException nfe) {
                    die("invalid hex value for \\x escape");
                }
                newstr.append(Character.toChars(value));
                if (saw_brace) { j++; }
                i += j-1;
                break; /* switch */
            }

            case 'u': {
                if (i+4 > oldstr.length()) {
                    die("string too short for \\u escape");
                }
                i++;
                int j;
                for (j = 0; j < 4; j++) {
                    /* this also handles the surrogate issue */
                    if (oldstr.charAt(i+j) > 127) {
                        die("illegal non-ASCII hex digit in \\u escape");
                    }
                }
                int value = 0;
                try {
                    value = Integer.parseInt( oldstr.substring(i, i+j), 16);
                } catch (NumberFormatException nfe) {
                    die("invalid hex value for \\u escape");
                }
                newstr.append(Character.toChars(value));
                i += j-1;
                break; /* switch */
            }

            case 'U': {
                if (i+8 > oldstr.length()) {
                    die("string too short for \\U escape");
                }
                i++;
                int j;
                for (j = 0; j < 8; j++) {
                    /* this also handles the surrogate issue */
                    if (oldstr.charAt(i+j) > 127) {
                        die("illegal non-ASCII hex digit in \\U escape");
                    }
                }
                int value = 0;
                try {
                    value = Integer.parseInt(oldstr.substring(i, i+j), 16);
                } catch (NumberFormatException nfe) {
                    die("invalid hex value for \\U escape");
                }
                newstr.append(Character.toChars(value));
                i += j-1;
                break; /* switch */
            }

            default:   newstr.append('\\');
                       newstr.append(Character.toChars(cp));
           /*
            * say(String.format(
            *       "DEFAULT unrecognized escape %c passed through",
            *       cp));
            */
                       break; /* switch */

        }
        saw_backslash = false;
    }

    /* weird to leave one at the end */
    if (saw_backslash) {
        newstr.append('\\');
    }

    return newstr.toString();
}

/*
 * Return a string "U+XX.XXX.XXXX" etc, where each XX set is the
 * xdigits of the logical Unicode code point. No bloody brain-damaged
 * UTF-16 surrogate crap, just true logical characters.
 */
 public final static
 String uniplus(String s) {
     if (s.length() == 0) {
         return "";
     }
     /* This is just the minimum; sb will grow as needed. */
     StringBuffer sb = new StringBuffer(2 + 3 * s.length());
     sb.append("U+");
     for (int i = 0; i < s.length(); i++) {
         sb.append(String.format("%X", s.codePointAt(i)));
         if (s.codePointAt(i) > Character.MAX_VALUE) {
             i++; /****WE HATES UTF-16! WE HATES IT FOREVERSES!!!****/
         }
         if (i+1 < s.length()) {
             sb.append(".");
         }
     }
     return sb.toString();
 }

private static final
void die(String foa) {
    throw new IllegalArgumentException(foa);
}

}
