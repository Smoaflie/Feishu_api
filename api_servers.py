import logging
import requests
import time
import ujson
import inspect

from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)

CONTENT_TYPE = "application/json; charset=utf-8"

TENANT_ACCESS_TOKEN_URI = "/auth/v3/tenant_access_token/internal"
USER_ACCESS_TOKEN_URI = "/authen/v2/oauth/token"
MESSAGE_URI = "/im/v1/messages"
CHAT_URL = "/im/v1/chats"
SPREADSHEET_URL_V2 = "/sheets/v2/spreadsheets"
SPREADSHEET_URL_V3 = "/sheets/v3/spreadsheets"
CONTACT_URL = "/contact/v3"
APPROVAL_URL = "/approval/v4"
TASK_URL = "/task/v2"


class APIContainer:
    """Api容器"""

    def __init__(self, app_id, app_secret, host):
        self.spreadsheet = SpreadsheetApiClient(app_id, app_secret, host)
        self.message = MessageApiClient(app_id, app_secret, host)
        self.chat = ChatApiClient(app_id, app_secret, host)
        self.contact = ContactApiClient(app_id, app_secret, host)
        self.cloud = CloudApiClient(app_id, app_secret, host)
        self.approval = ApprovalApiClient(app_id, app_secret, host)
        self.task = TaskApiClient(app_id, app_secret, host)

    def __getattr__(self, name):
        return self._clients.get(name, None)  # 访问不到返回 None，避免报错


def _send_with_retries(
    method,
    max_retries: int = 3,  # 最大重试次数
    retry_delay: int = 2,  # 重试间隔（秒）
    *args,
    **kwargs,
):
    """发送http请求且失败后自动重试"""
    # 通过栈信息获取调用函数名
    stack = inspect.stack()
    caller_function_name = stack[1].function
    for attempt in range(max_retries):
        try:
            resp = method(*args, **kwargs)
            ApiClient._check_error_response(resp)

            logger.info(f"func<{caller_function_name}> handle success: {resp}")
            return resp.json()
        except LarkException as e:
            raise
        except HTTPError as e:
            logger.warning(
                f"func<{caller_function_name}> 请求失败，尝试重试 {attempt + 1}/"
                f"{max_retries}，错误信息: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay)  # 等待一段时间再重试
            else:
                logger.error(
                    f"func<{caller_function_name}> 超过最大重试次数，错误信息: {e}"
                )
                raise  # 超过最大重试次数，抛出异常


class ApiClient(object):
    """飞书Api基类."""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        lark_host: str = "https://open.feishu.cn",
    ):
        """初始化函数."""
        self._app_id = app_id
        self._app_secret = app_secret
        self._lark_open_api_host = lark_host + "/open-apis"
        self._tenant_access_token = ""
        self._user_access_token = ""
        self._identity = "tenant"  # 默认以应用身份调用API
        self._oauth_code = ""  # 授权码 可以以用户身份调用 OpenAPI
        self._user_access_token_validity = 0  # user凭证有效时间(秒时间戳)

    def set_oauth_code(self, code):
        self._oauth_code = code
        return self

    def set_identity(self, identity):
        if identity in ("tenant", "user"):
            self._identity = identity
            return self
        else:
            raise ValueError("Unknown identity {%s} for app" % identity)

    @property
    def tenant_access_token(self):
        """应用的tenant_access_token"""
        return self._tenant_access_token

    @property
    def user_access_token(self):
        """自定义user_access_token"""
        return self._user_access_token

    @property
    def authorization(self):
        """Authorization字段"""
        if self._identity == "tenant":
            self._authorize_tenant_access_token()
            return "Bearer " + self.tenant_access_token
        elif self._identity == "user":
            self._authorize_user_access_token()
            return "Bearer " + self.user_access_token
        else:
            raise ValueError("Unknown identity {%s} for app" % self._identity)

    def _authorize_tenant_access_token(self):
        """
        通过此接口获取 tenant_access_token.

        doc link:
            https://open.feishu.cn/document/server-docs/authentication-management/access-token/tenant_access_token_internal
        """
        url = "{}{}".format(self._lark_open_api_host, TENANT_ACCESS_TOKEN_URI)
        req_body = {"app_id": self._app_id, "app_secret": self._app_secret}
        response = requests.post(url, json=req_body)
        self._check_error_response(response)
        self._tenant_access_token = response.json().get("tenant_access_token")

    def _authorize_user_access_token(self):
        """
        通过此接口获取 user_access_token.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/authentication-management/access-token/refresh-user-access-token
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/authentication-management/access-token/get-user-access-token
        """
        # 判断原凭证是否到期
        if time.time() < self._user_access_token_validity:
            return self.authorization
        else:
            # 凭证到期，如refresh_token仍有效，直接获取新凭证
            if time.time() < self._user_access_token_refresh_token_validity:
                url = "{}{}".format(self._lark_open_api_host, USER_ACCESS_TOKEN_URI)
                req_body = {
                    "grant_type": "refresh_token",
                    "client_id": self._app_id,
                    "client_secret": self._app_secret,
                    "refresh_token": self._user_access_token_refresh_token,
                }
                response = requests.post(url, req_body)
            # 如refresh_token也到期，则通过授权码重新获取
            else:
                url = "{}{}".format(self._lark_open_api_host, USER_ACCESS_TOKEN_URI)
                req_body = {
                    "grant_type": "authorization_code",
                    "client_id": self._app_id,
                    "client_secret": self._app_secret,
                    "code": self._oauth_code,
                }
                response = requests.post(url, req_body)
                self._oauth_code = None  # 授权码仅能使用一次
            self._check_error_response(response)
            self._user_access_token = response.json().get("access_token")
            self._user_access_token_validity = time.time() + response.json().get(
                "expires_in"
            )
            self._user_access_token_refresh_token = response.json().get("refresh_token")
            self._user_access_token_refresh_token_validity = (
                time.time() + response.json().get("refresh_token_expires_in")
            )

    @staticmethod
    def _check_error_response(resp):
        """检查响应是否包含错误信息."""
        try:
            response_dict = resp.json()
        except requests.exceptions.JSONDecodeError:
            logger.error(f"Request failed with status code {resp.status_code}")
            return
        code = response_dict.get("code", -1)
        if code != 0:
            if code == -1:
                resp.raise_for_status()
            raise LarkException(code=code, msg=response_dict.get("msg"))


class MessageApiClient(ApiClient):
    """服务端API 消息."""

    def send_text_with_user_id(self, user_id: str, content: str) -> dict:
        """通过user_id向用户发送文本."""
        return self.send("user_id", user_id, "text", {"text": content})

    def send_interactive_with_user_id(self, user_id: str, content: dict) -> dict:
        """通过user_id向用户发送消息卡片."""
        return self.send("user_id", user_id, "interactive", content)

    def send(
        self, receive_id_type: str, receive_id: str, msg_type: str, content: dict
    ) -> dict:
        """
        发送消息.

        调用该接口向指定用户或者群聊发送消息。支持发送的消息类型包括文本、富文本、
        卡片、群名片、个人名片、图片、视频、音频、文件以及表情包等。

        doc link:
            https://open.feishu.cn/document/server-docs/im-v1/message/create
        """
        url = "{}{}?receive_id_type={}".format(
            self._lark_open_api_host, MESSAGE_URI, receive_id_type
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {
            "receive_id": receive_id,
            "msg_type": msg_type,
            "content": ujson.dumps(content),
        }
        return _send_with_retries(
            requests.post, url=url, headers=headers, json=req_body
        )

    def recall(self, message_id: str) -> dict:
        """
        撤回消息.

        doc link:
            https://open.feishu.cn/document/server-docs/im-v1/message/delete
        """
        url = "{}{}/{}".format(self._lark_open_api_host, MESSAGE_URI, message_id)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        return _send_with_retries(requests.get, url=url, headers=headers)

    def delay_update_message_card(self, token: str, card: dict) -> dict:
        """
        延时更新消息卡片.

        用户与卡片进行交互后，飞书服务器会发送卡片回传交互回调，服务器需要在接收回调
        的 3 秒内以 HTTP 200 状态码响应该回调，在响应时设置 HTTP Body 为 "{}"
        或者返回自定义 Toast 结构体，详情参考配置卡片交互。

        延时更新卡片必须在响应回调之后进行，并行执行或提前执行会出现更新失败的情况。

        延时更新所需的 token 有效期为 30 分钟，超时则无法更新卡片，且同一个 token
        只能使用 2 次，超过使用次数则无法更新卡片。

        其余信息请参考文档

        doc link:
            https://open.feishu.cn/document/server-docs/im-v1/message-card/delay-update-message-card
        """
        url = "{}/interactive/v1/card/update".format(self._lark_open_api_host)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"token": token, "card": card}
        return _send_with_retries(
            requests.post, url=url, headers=headers, json=req_body
        )

    def list(
        self,
        container_id_type: str,
        container_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
        sort_type: str | None = None,
        page_size: int = 20,
        page_token: str | None = None,
    ) -> dict:
        """
        获取会话历史消息.

        doc link:
            https://open.feishu.cn/document/server-docs/im-v1/message/list
        """
        url = "{}{}".format(self._lark_open_api_host, MESSAGE_URI)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {
            "container_id_type": container_id_type,
            "container_id": container_id,
            "start_time": start_time,
            "end_time": end_time,
            "sort_type": sort_type,
            "page_size": page_size,
            "page_token": page_token,
        }
        req_body = {}
        return _send_with_retries(
            requests.get, url=url, params=params, headers=headers, json=req_body
        )


class ChatApiClient(ApiClient):
    """服务端API 群组."""

    def get_members(
        self,
        chat_id: str,
        member_id_type: str = "user_id",
        page_size: int = 20,
        page_token: str | None = None,
    ) -> dict:
        """
        获取群成员列表.

        doc link:
            https://open.feishu.cn/document/server-docs/group/chat-member/get
        """
        url = "{}{}/{}/members".format(self._lark_open_api_host, CHAT_URL, chat_id)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {
            "member_id_type": member_id_type,
            "page_size": page_size,
            "page_token": page_token,
        }
        req_body = {}
        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )


class SpreadsheetApiClient(ApiClient):
    """服务端API 电子表格."""

    def create(self, spreadsheet_token: str, req_list: dict) -> dict:
        """
        操作工作表.

        根据电子表格的 token 对工作表进行操作，包括增加工作表、复制工作表、删除工作表。

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/spreadsheet-sheet/operate-sheets
        """
        url = "{}{}/{}/sheets_batch_update".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheet_token
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"requests": req_list}
        return _send_with_retries(
            requests.post, url=url, headers=headers, data=ujson.dumps(req_body)
        )

    def update(self, spreadsheet_token: str, properties: dict) -> dict:
        """
        更新工作表属性.

        更新电子表格中的工作表。支持更新工作表的标题、位置，和隐藏、冻结、保护等属性。

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/spreadsheet-sheet/update-sheet-properties
        """
        url = "{}{}/{}/sheets_batch_update".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheet_token
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"requests": {"updateSheet": {"properties": properties}}}
        return _send_with_retries(
            requests.post, url=url, headers=headers, data=ujson.dumps(req_body)
        )

    def query(self, spreadsheet_token: str) -> dict:
        """
        获取电子表格信息.

        根据电子表格 token 获取表格中所有工作表及其属性信息，包括
        工作表 ID、标题、索引位置、是否被隐藏等。

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/spreadsheet-sheet/query
        """
        url = "{}{}/{}/sheets/query".format(
            self._lark_open_api_host, SPREADSHEET_URL_V3, spreadsheet_token
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        return _send_with_retries(requests.get, url=url, headers=headers)

    def reading_a_single_range(
        self, spreadsheetToken: str, sheetId: str, range: str
    ) -> dict:
        """
        读取电子表格中单个指定范围的数据.

        Args:
            spreadsheetToken: 电子表格token
            sheetId:  工作表ID
            range: 查询范围。格式为"<开始位置>:<结束位置>"。其中：
                <开始位置>:<结束位置> 为工作表中单元格的范围，数字表示行索引，
                字母表示列索引。如 A2:B2 表示该工作表第 2 行的 A 列到 B 列。
                range支持四种写法，详情参考电子表格概述

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/reading-a-single-range
        """
        url = "{}{}/{}/values/{}".format(
            self._lark_open_api_host,
            SPREADSHEET_URL_V2,
            spreadsheetToken,
            f"{sheetId}!{range}",
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        return _send_with_retries(requests.get, url=url, headers=headers)

    def write_date_to_a_single_range(
        self, spreadsheetToken: str, sheetId: str, range: str, values: list
    ) -> dict:
        """
        向单个范围写入数据.

        向电子表格某个工作表的单个指定范围中写入数据。
        若指定范围已内有数据，将被新写入的数据覆盖。

        Args:
            spreadsheetToken: 电子表格token
            sheetId:  工作表ID
            range: 查询范围。格式为"<开始位置>:<结束位置>"。其中：
                <开始位置>:<结束位置> 为工作表中单元格的范围，数字表示行索引，
                字母表示列索引。如 A2:B2 表示该工作表第 2 行的 A 列到 B 列。
                range支持四种写法，详情参考电子表格概述
            values: 写入的数据

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/write-data-to-a-single-range
        """
        url = "{}{}/{}/values".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheetToken
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"valueRange": {"range": f"{sheetId}!{range}", "values": values}}
        return _send_with_retries(
            requests.put, url=url, headers=headers, data=ujson.dumps(req_body)
        )

    def delete_rows_or_columns(
        self,
        spreadsheetToken: str,
        sheetId: str,
        majorDimension: str,
        startIndex: int,
        endIndex: int,
    ) -> dict:
        """
        删除电子表格中的指定行或列.

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/sheet-rowcol/-delete-rows-or-columns
        """
        url = "{}{}/{}/dimension_range".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheetToken
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {
            "dimension": {
                "sheetId": sheetId,
                "majorDimension": majorDimension,
                "startIndex": startIndex,
                "endIndex": endIndex,
            }
        }
        return _send_with_retries(
            requests.delete, url=url, headers=headers, data=ujson.dumps(req_body)
        )

    def merge_cells(
        self,
        spreadsheetToken: str,
        sheetId: str,
        range: str,
        mergeType: str,
    ) -> dict:
        """
        合并电子表格工作表中的单元格。

        mergeType:
            MERGE_ALL：合并所有单元格，即将选定区域内的所有单元格合并成一个单元格
            MERGE_ROWS：按行合并，即在选定的区域内，将同一行相邻的单元格合并成一个单元格
            MERGE_COLUMNS：按列合并，即在选定的区域内，将同一列中相邻的单元格合并成一个单元格

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/merge-cells
        """
        url = "{}{}/{}/merge_cells".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheetToken
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"mergeType": mergeType, "range": f"{sheetId}!{range}"}
        return _send_with_retries(
            requests.post, url=url, headers=headers, data=ujson.dumps(req_body)
        )

    def set_style(
        self,
        spreadsheetToken: str,
        sheetId: str,
        range: str,
        style: dict,
    ) -> dict:
        """
        设置单元格中数据的样式。支持设置字体、背景、边框等样式。

        style请参照官方文档

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/set-cell-style
        """
        url = "{}{}/{}/style".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheetToken
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"appendStyle": {"range": f"{sheetId}!{range}", "style": style}}
        return _send_with_retries(
            requests.put, url=url, headers=headers, data=ujson.dumps(req_body)
        )

    def update_dimension_range(
        self,
        spreadsheetToken: str,
        sheetId: str,
        majorDimension: str,
        startIndex: int,
        endIndex: int,
        visible: bool = True,
        fixedSize: int = 50,
    ) -> dict:
        """
        该接口用于更新设置电子表格中行列的属性，包括是否隐藏行列和设置行高列宽。

        style请参照官方文档

        doc link:
            https://open.feishu.cn/document/server-docs/docs/sheets-v3/sheet-rowcol/update-rows-or-columns
        """
        url = "{}{}/{}/dimension_range".format(
            self._lark_open_api_host, SPREADSHEET_URL_V2, spreadsheetToken
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {
            "dimension": {
                "sheetId": sheetId,
                "majorDimension": majorDimension,
                "startIndex": startIndex,
                "endIndex": endIndex,
            },
            "dimensionProperties": {"visible": visible, "fixedSize": fixedSize},
        }
        return _send_with_retries(
            requests.put, url=url, headers=headers, data=ujson.dumps(req_body)
        )


class ContactApiClient(ApiClient):
    """服务端API 通讯录."""

    def get_scopes(
        self,
        user_id_type: str = "open_id",
        department_id_type: str = "open_department_id",
        page_token: str | None = None,
    ) -> dict:
        """
        获取通讯录授权范围.

        调用该接口获取当前应用被授权可访问的通讯录范围，包括
        可访问的部门列表、用户列表和用户组列表。

        doc link:
            https://open.feishu.cn/document/server-docs/contact-v3/scope/list
        """
        url = "{}{}/scopes".format(self._lark_open_api_host, CONTACT_URL)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {
            "user_id_type": user_id_type,
            "department_id_type": department_id_type,
            "page_token": page_token,
        }
        return _send_with_retries(requests.get, url=url, headers=headers, params=params)

    def get_users_batch(self, user_ids: list, user_id_type: str = "open_id") -> dict:
        """
        批量获取用户信息.

        调用该接口获取通讯录内一个或多个用户的信息，包括用户 ID、
        名称、邮箱、手机号、状态以及所属部门等信息。

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/contact-v3/user/batch
        """
        # 批量获取用户信息
        url = "{}{}/users/batch".format(self._lark_open_api_host, CONTACT_URL)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {
            "user_ids": user_ids,
            "user_id_type": user_id_type,
        }
        return _send_with_retries(requests.get, url=url, headers=headers, params=params)


class CloudApiClient(ApiClient):
    """服务端API 云空间"""

    def search_docs(
        self,
        search_key: str,
        count: int = 50,
        offset: int = 0,
        owner_ids: list[str] = [],
        chat_ids: list[str] = [],
        docs_types: list[str] = [],
    ) -> dict:
        """
        搜索云文档.

        doc link:
            https://open.feishu.cn/document/server-docs/docs/drive-v1/search/document-search
        """
        url = "{}/suite/docs-api/search/object".format(self._lark_open_api_host)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {
            "search_key": search_key,
            "count": count,
            "offset": offset,
            "owner_ids": owner_ids,
            "chat_ids": chat_ids,
            "docs_types": docs_types,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, json=req_body
        )

    def query_docs_metadata(
        self,
        doc_token: list[str],
        doc_type: list[str],
        user_id_type: str = "open_id",
        with_url: bool = False,
    ) -> dict:
        """
        获取文件元数据.

        该接口用于根据文件 token 获取其元数据，包括标题、
        所有者、创建时间、密级、访问链接等数据。

        doc link:
            https://open.feishu.cn/document/server-docs/docs/drive-v1/file/batch_query
        """
        url = "{}/drive/v1/metas/batch_query".format(self._lark_open_api_host)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        request_docs = []
        for token, type in zip(doc_token, doc_type):
            request_docs.append(
                {"doc_token": token, "doc_type": type, "with_url": with_url}
            )
        req_body = {"request_docs": request_docs}

        return _send_with_retries(
            requests.post, url=url, headers=headers, json=req_body, params=params
        )


class ApprovalApiClient(ApiClient):
    """服务端API 审批"""

    def create_instance(self, approval_code: str, form: str, user_id: str) -> dict:
        """
        创建审批实例.

        doc link:
            https://open.feishu.cn/document/server-docs/approval-v4/instance/create
        """
        url = "{}{}/instances".format(self._lark_open_api_host, APPROVAL_URL)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {"approval_code": approval_code, "user_id": user_id, "form": form}

        return _send_with_retries(
            requests.post, url=url, headers=headers, json=req_body
        )

    def subscribe(self, approval_code: str) -> dict:
        """
        订阅审批事件.

        应用订阅 approval_code 后，该应用就可以收到该审批定义对应实例的事件通知。
        同一应用只需要订阅一次，无需重复订阅。

        doc link:
            https://open.feishu.cn/document/server-docs/approval-v4/event/event-interface/subscribe
        """
        url = "{}{}/approvals/{}/subscribe".format(
            self._lark_open_api_host, APPROVAL_URL, approval_code
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }

        return _send_with_retries(requests.post, url=url, headers=headers)

    def get_instance(self, instance_id: str) -> dict:
        """
        获取单个审批实例详情.

        doc link:
            https://open.feishu.cn/document/server-docs/approval-v4/instance/get
        """
        url = "{}{}/instances/{}".format(
            self._lark_open_api_host, APPROVAL_URL, instance_id
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }

        return _send_with_retries(requests.get, url=url, headers=headers)


class TaskApiClient(ApiClient):
    """服务端API 任务"""

    """任务"""

    def create_task(
        self,
        summary: str,
        user_id_type: str = "user_id",
        description: str | None = None,
        due: dict | None = None,
        origin: dict | None = None,
        extra: str | None = None,
        completed_at: str = 0,
        members: list | None = None,
        repeat_rule: str | None = None,
        custom_complete: dict | None = None,
        tasklists: list | None = None,
        client_token: str | None = None,
        start: dict | None = None,
        reminders: list | None = None,
        mode: int = 2,
        is_milestone: bool = False,
        custom_fields: list | None = None,
    ) -> dict:
        """
        创建任务.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/create
        """
        url = "{}{}/tasks".format(self._lark_open_api_host, TASK_URL)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "summary": summary,
            "description": description,
            "due": due,
            "origin": origin,
            "extra": extra,
            "completed_at": completed_at,
            "members": members,
            "repeat_rule": repeat_rule,
            "custom_complete": custom_complete,
            "tasklists": tasklists,
            "client_token": client_token,
            "start": start,
            "reminders": reminders,
            "mode": mode,
            "is_milestone": is_milestone,
            "custom_fields": custom_fields,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def get_task_detail(
        self,
        guid: str,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        获取任务详情.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/get
        """
        url = "{}{}/tasks/{}".format(self._lark_open_api_host, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {}

        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )

    def patch_task(
        self,
        guid: str,
        update_fields: list[str],
        user_id_type: str = "user_id",
        task: dict | None = None,
    ) -> dict:
        """
        更新任务.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/patch
        """
        url = "{}{}/tasks/{}".format(self._lark_open_api_host, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {"task": task, "update_fields": update_fields}

        return _send_with_retries(
            requests.patch, url=url, headers=headers, params=params, json=req_body
        )

    def delete_task(
        self,
        guid: str,
    ) -> dict:
        """
        删除任务.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/delete
        """
        url = "{}{}/tasks/{}".format(self._lark_open_api_host, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {}

        return _send_with_retries(
            requests.delete, url=url, headers=headers, params=params, json=req_body
        )

    def add_task_members(
        self,
        guid: str,
        members: list,
        user_id_type: str = "user_id",
        client_token: str | None = None,
    ) -> dict:
        """
        添加任务成员.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/add_members
        """
        url = "{}{}/tasks/{}/add_members".format(self._lark_open_api_host, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {"members": members, "client_token": client_token}

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def remove_task_members(
        self,
        guid: str,
        members: list,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        删除任务成员.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/remove_members
        """
        url = "{}{}/tasks/{}/remove_members".format(self._lark_open_api_host, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "members": members,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def get_task_list(
        self,
        page_size: int = 50,
        page_token: str | None = None,
        completed: bool | None = None,
        type: str | None = None,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        删除任务成员.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/list
        """
        url = "{}{}/tasks".format(self._lark_open_api_host)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {
            "page_size": page_size,
            "page_token": page_token,
            "completed": completed,
            "type": type,
            "user_id_type": user_id_type,
        }
        req_body = {}

        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )

    def get_task_inventory(
        self,
        guid: str,
    ) -> dict:
        """
        列取任务所在清单.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/tasklists
        """
        url = "{}{}/tasks/{}/tasklists".format(self._lark_open_api_host, TASK_URL, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {}

        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )

    def add_inventory_task(
        self,
        task_guid: str,
        tasklist_guid: str,
        section_guid: str | None = None,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        任务加入清单.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/add_tasklist
        """
        url = "{}{}/tasks/{}/add_tasklist".format(
            self._lark_open_api_host, TASK_URL, task_guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "tasklist_guid": tasklist_guid,
            "section_guid": section_guid,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def remove_inventory_task(
        self,
        task_guid: str,
        tasklist_guid: str,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        任务移出清单.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/add_tasklist
        """
        url = "{}{}/tasks/{}/remove_tasklist".format(
            self._lark_open_api_host, TASK_URL, task_guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "tasklist_guid": tasklist_guid,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def add_task_dependencies(self, guid: str, dependencies: list) -> dict:
        """
        添加依赖.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/add_dependencies
        """
        url = "{}{}/tasks/{}/add_dependencies".format(
            self._lark_open_api_host, TASK_URL, guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {
            "dependencies": dependencies,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def remove_task_dependencies(self, guid: str, dependencies: list) -> dict:
        """
        移除依赖.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task/remove_dependencies
        """
        url = "{}{}/tasks/{}/remove_dependencies".format(
            self._lark_open_api_host, TASK_URL, guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {
            "dependencies": dependencies,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    """子任务"""

    def create_subtask(
        self,
        task_guid: str,
        summary: str,
        user_id_type: str = "user_id",
        description: str | None = None,
        due: dict | None = None,
        origin: dict | None = None,
        extra: str | None = None,
        completed_at: str = 0,
        members: list | None = None,
        repeat_rule: str | None = None,
        custom_complete: dict | None = None,
        tasklists: list | None = None,
        client_token: str | None = None,
        start: dict | None = None,
        reminders: list | None = None,
    ) -> dict:
        """
        创建子任务.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task-subtask/create
        """
        url = "{}{}/tasks/{}/subtasks".format(
            self._lark_open_api_host, TASK_URL, task_guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "summary": summary,
            "description": description,
            "due": due,
            "origin": origin,
            "extra": extra,
            "completed_at": completed_at,
            "members": members,
            "repeat_rule": repeat_rule,
            "custom_complete": custom_complete,
            "tasklists": tasklists,
            "client_token": client_token,
            "start": start,
            "reminders": reminders,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def get_task_subtasklist(
        self,
        guid: str,
        page_size: int = 50,
        page_token: str | None = None,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        获取任务的子任务列表.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/task-subtask/list
        """
        url = "{}{}/tasks/{}/subtasks".format(self._lark_open_api_host, TASK_URL, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {
            "page_size": page_size,
            "page_token": page_token,
            "user_id_type": user_id_type,
        }
        req_body = {}

        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )

    """清单"""

    def create_inventory(
        self, name: str, members: list | None = None, user_id_type: str = "user_id"
    ) -> dict:
        """
        创建清单.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/create
        """
        url = "{}{}/tasklists".format(self._lark_open_api_host, TASK_URL)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "name": name,
            "members": members,
        }

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def get_inventory_detail(self, guid: str, user_id_type: str = "user_id") -> dict:
        """
        获取清单详情.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/get
        """
        url = "{}{}/tasklists/{}".format(self._lark_open_api_host, TASK_URL, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {"user_id_type": user_id_type}

        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )

    def patch_inventory(
        self,
        guid: str,
        tasklist: list,
        update_fields: list[str],
        user_id_type: str = "user_id",
        origin_owner_to_role: str = "none",
    ) -> dict:
        """
        更新清单.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/patch
        """
        url = "{}{}/tasklists/{}".format(self._lark_open_api_host, TASK_URL, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {
            "tasklist": tasklist,
            "update_fields": update_fields,
            "origin_owner_to_role": origin_owner_to_role,
        }

        return _send_with_retries(
            requests.patch, url=url, headers=headers, params=params, json=req_body
        )

    def delete_task_inventory(
        self,
        guid: str,
    ) -> dict:
        """
        删除清单列表.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/delete
        """
        url = "{}{}/tasklists/{}".format(self._lark_open_api_host, TASK_URL, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {}

        return _send_with_retries(
            requests.delete, url=url, headers=headers, params=params, json=req_body
        )

    def add_inventory_member(
        self,
        guid: str,
        members: list,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        添加清单成员.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/add_members
        """
        url = "{}{}/tasklists/{}/add_members".format(
            self._lark_open_api_host, TASK_URL, guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {"members": members}

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def remove_inventory_member(
        self,
        guid: str,
        members: list,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        移除清单成员.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/remove_members
        """
        url = "{}{}/tasklists/{}/add_members".format(
            self._lark_open_api_host, TASK_URL, guid
        )
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {"user_id_type": user_id_type}
        req_body = {"members": members}

        return _send_with_retries(
            requests.post, url=url, headers=headers, params=params, json=req_body
        )

    def get_inventory_tasks(
        self,
        guid: str,
        page_size: int = 50,
        page_token: str | None = None,
        completed: bool = True,
        created_from: str | None = None,
        created_to: str | None = None,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        获取清单任务列表.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/tasks
        """
        url = "{}{}/tasklists/{}".format(self._lark_open_api_host, TASK_URL, guid)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        params = {}
        req_body = {
            "page_size": page_size,
            "page_token": page_token,
            "completed": completed,
            "created_from": created_from,
            "created_to": created_to,
            "user_id_type": user_id_type,
        }
        return _send_with_retries(
            requests.get, url=url, headers=headers, params=params, json=req_body
        )

    def get_inventory_list(
        self,
        page_size: int = 50,
        page_token: str | None = None,
        user_id_type: str = "user_id",
    ) -> dict:
        """
        获取清单列表.

        doc link:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/task-v2/tasklist/list
        """
        url = "{}{}/tasklists".format(self._lark_open_api_host, TASK_URL)
        headers = {
            "Authorization": self.authorization,
            "Content-Type": CONTENT_TYPE,
        }
        req_body = {
            "page_size": page_size,
            "page_token": page_token,
            "user_id_type": user_id_type,
        }
        return _send_with_retries(requests.get, url=url, headers=headers, json=req_body)


class LarkException(Exception):
    """自定义飞书异常."""

    def __init__(self, code=0, msg=None):
        self.code = code
        self.msg = msg
        logger.error(f"LarkException: {code}:{msg}")

    def __str__(self) -> str:
        return "{}:{}".format(self.code, self.msg)

    __repr__ = __str__
