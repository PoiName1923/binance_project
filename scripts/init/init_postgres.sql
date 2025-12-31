CREATE DATABASE analyst_platform;

-- Tạo schema USER (quản lý đăng nhập và thông tin người dùng)
-- Schema: users_profile
-- Table user
username
password_hash
create_at
update_at
-- Table profile
lastname
firstname
gender
dob
addr
-- Tạo schema LOGS (quản lý các câu hỏi của người dùng và câu trả lời của AI)