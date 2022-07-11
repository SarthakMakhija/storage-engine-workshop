package db

type Slice struct {
	contents []byte
}

var emptySlice = Slice{}

func NilSlice() Slice {
	return emptySlice
}

func NewSlice(contents []byte) Slice {
	return Slice{contents: contents}
}

func (slice Slice) GetRawContent() []byte {
	return slice.contents
}

func (slice Slice) AsString() string {
	return string(slice.contents)
}
