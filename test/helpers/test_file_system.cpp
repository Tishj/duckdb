#include "test_file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

TestFileSystem::TestFileSystem(const string &test_directory) : test_directory(test_directory), fs(make_uniq<VirtualFileSystem>()) {
}

void VerifyAccess(const string &path, const string &test_directory) {
	// Check that we don't touch files outside of our control
	if (!StringUtil::StartsWith(path, test_directory)) {
		throw InternalException("TEST TRIED TO ACCESS FILE OUTSIDE OF TEST DIRECTORY: %s", path);
	}
}

unique_ptr<FileHandle> TestFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	if (flags.CreateFileIfNotExists() || flags.OverwriteExistingFile()) {
		VerifyAccess(path, test_directory);
	}
	return fs->OpenFile(path, flags, opener);
}

void TestFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Read(handle, buffer, nr_bytes, location);
}

int64_t TestFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return fs->Read(handle, buffer, nr_bytes);
}

void TestFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Write(handle, buffer, nr_bytes, location);
}

int64_t TestFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return fs->Write(handle, buffer, nr_bytes);
}

int64_t TestFileSystem::GetFileSize(FileHandle &handle) {
	return fs->GetFileSize(handle);
}

time_t TestFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return fs->GetLastModifiedTime(handle);
}

FileType TestFileSystem::GetFileType(FileHandle &handle) {
	return fs->GetFileType(handle);
}

void TestFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	fs->Truncate(handle, new_size);
}

void TestFileSystem::FileSync(FileHandle &handle) {
	fs->FileSync(handle);
}

bool TestFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return fs->DirectoryExists(directory, opener);
}

void TestFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	fs->CreateDirectory(directory, opener);
}

void TestFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	fs->RemoveDirectory(directory, opener);
}

bool TestFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	return fs->ListFiles(directory, callback, opener);
}

void TestFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	fs->MoveFile(source, target, opener);
}

bool TestFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return fs->FileExists(filename, opener);
}

bool TestFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return fs->IsPipe(filename, opener);
}

void TestFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	fs->RemoveFile(filename, opener);
}

vector<string> TestFileSystem::Glob(const string &path, FileOpener *opener) {
	return fs->Glob(path, opener);
}

void TestFileSystem::RegisterSubSystem(unique_ptr<FileSystem> fs) {
	fs->RegisterSubSystem(std::move(fs));
}

void TestFileSystem::UnregisterSubSystem(const string &name) {
	fs->UnregisterSubSystem(name);
}

void TestFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	fs->RegisterSubSystem(compression_type, std::move(fs));
}

vector<string> TestFileSystem::ListSubSystems() {
	return fs->ListSubSystems();
}

std::string TestFileSystem::GetName() const {
	return fs->GetName();
}

void TestFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	fs->SetDisabledFileSystems(names);
}

string TestFileSystem::PathSeparator(const string &path) {
	return fs->PathSeparator(path);
}

} // namespace duckdb
