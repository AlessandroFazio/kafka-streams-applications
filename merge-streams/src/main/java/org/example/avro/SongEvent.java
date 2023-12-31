/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.example.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class SongEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -3909061495632294943L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SongEvent\",\"namespace\":\"org.example.avro\",\"fields\":[{\"name\":\"artist\",\"type\":\"string\"},{\"name\":\"title\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<SongEvent> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<SongEvent> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<SongEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<SongEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<SongEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this SongEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a SongEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a SongEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static SongEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.CharSequence artist;
  private java.lang.CharSequence title;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public SongEvent() {}

  /**
   * All-args constructor.
   * @param artist The new value for artist
   * @param title The new value for title
   */
  public SongEvent(java.lang.CharSequence artist, java.lang.CharSequence title) {
    this.artist = artist;
    this.title = title;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return artist;
    case 1: return title;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: artist = (java.lang.CharSequence)value$; break;
    case 1: title = (java.lang.CharSequence)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'artist' field.
   * @return The value of the 'artist' field.
   */
  public java.lang.CharSequence getArtist() {
    return artist;
  }


  /**
   * Sets the value of the 'artist' field.
   * @param value the value to set.
   */
  public void setArtist(java.lang.CharSequence value) {
    this.artist = value;
  }

  /**
   * Gets the value of the 'title' field.
   * @return The value of the 'title' field.
   */
  public java.lang.CharSequence getTitle() {
    return title;
  }


  /**
   * Sets the value of the 'title' field.
   * @param value the value to set.
   */
  public void setTitle(java.lang.CharSequence value) {
    this.title = value;
  }

  /**
   * Creates a new SongEvent RecordBuilder.
   * @return A new SongEvent RecordBuilder
   */
  public static org.example.avro.SongEvent.Builder newBuilder() {
    return new org.example.avro.SongEvent.Builder();
  }

  /**
   * Creates a new SongEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new SongEvent RecordBuilder
   */
  public static org.example.avro.SongEvent.Builder newBuilder(org.example.avro.SongEvent.Builder other) {
    if (other == null) {
      return new org.example.avro.SongEvent.Builder();
    } else {
      return new org.example.avro.SongEvent.Builder(other);
    }
  }

  /**
   * Creates a new SongEvent RecordBuilder by copying an existing SongEvent instance.
   * @param other The existing instance to copy.
   * @return A new SongEvent RecordBuilder
   */
  public static org.example.avro.SongEvent.Builder newBuilder(org.example.avro.SongEvent other) {
    if (other == null) {
      return new org.example.avro.SongEvent.Builder();
    } else {
      return new org.example.avro.SongEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for SongEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<SongEvent>
    implements org.apache.avro.data.RecordBuilder<SongEvent> {

    private java.lang.CharSequence artist;
    private java.lang.CharSequence title;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.example.avro.SongEvent.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.artist)) {
        this.artist = data().deepCopy(fields()[0].schema(), other.artist);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing SongEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(org.example.avro.SongEvent other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.artist)) {
        this.artist = data().deepCopy(fields()[0].schema(), other.artist);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'artist' field.
      * @return The value.
      */
    public java.lang.CharSequence getArtist() {
      return artist;
    }


    /**
      * Sets the value of the 'artist' field.
      * @param value The value of 'artist'.
      * @return This builder.
      */
    public org.example.avro.SongEvent.Builder setArtist(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.artist = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'artist' field has been set.
      * @return True if the 'artist' field has been set, false otherwise.
      */
    public boolean hasArtist() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'artist' field.
      * @return This builder.
      */
    public org.example.avro.SongEvent.Builder clearArtist() {
      artist = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'title' field.
      * @return The value.
      */
    public java.lang.CharSequence getTitle() {
      return title;
    }


    /**
      * Sets the value of the 'title' field.
      * @param value The value of 'title'.
      * @return This builder.
      */
    public org.example.avro.SongEvent.Builder setTitle(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.title = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'title' field has been set.
      * @return True if the 'title' field has been set, false otherwise.
      */
    public boolean hasTitle() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'title' field.
      * @return This builder.
      */
    public org.example.avro.SongEvent.Builder clearTitle() {
      title = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SongEvent build() {
      try {
        SongEvent record = new SongEvent();
        record.artist = fieldSetFlags()[0] ? this.artist : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.title = fieldSetFlags()[1] ? this.title : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<SongEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<SongEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<SongEvent>
    READER$ = (org.apache.avro.io.DatumReader<SongEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.artist);

    out.writeString(this.title);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.artist = in.readString(this.artist instanceof Utf8 ? (Utf8)this.artist : null);

      this.title = in.readString(this.title instanceof Utf8 ? (Utf8)this.title : null);

    } else {
      for (int i = 0; i < 2; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.artist = in.readString(this.artist instanceof Utf8 ? (Utf8)this.artist : null);
          break;

        case 1:
          this.title = in.readString(this.title instanceof Utf8 ? (Utf8)this.title : null);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










