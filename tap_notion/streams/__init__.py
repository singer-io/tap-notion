from tap_notion.streams.pages import Pages
from tap_notion.streams.databases import Databases
from tap_notion.streams.users import Users
from tap_notion.streams.blocks import Blocks
from tap_notion.streams.comments import Comments
from tap_notion.streams.bot_user import BotUser
from tap_notion.streams.file_uploads import FileUpload
from tap_notion.streams.user import User
from tap_notion.streams.file_uploads import FileUpload as FileUploadDetail
from tap_notion.streams.file_uploads_list import FileUploads
from tap_notion.streams.file_uploads_list import FileUploads
from tap_notion.streams.block_children import BlockChildren
from tap_notion.streams.page_property import PagesProperty

STREAMS = {
    "pages": Pages,
    "databases": Databases,
    "users": Users,
    "blocks": Blocks,
    "comments": Comments,
    "bot_user": BotUser,
    "file_upload_detail": FileUploadDetail,
    "file_uploads_list": FileUploads,
    "user": User,
    "block_children": BlockChildren,
    "pages_property": PagesProperty,
}
