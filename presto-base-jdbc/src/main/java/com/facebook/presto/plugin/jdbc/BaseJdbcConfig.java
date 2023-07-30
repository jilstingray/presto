/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

public class BaseJdbcConfig
{
    static final int MAX_ALLOWED_WRITE_BATCH_SIZE = 1000000;
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String userCredentialName;
    private String passwordCredentialName;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);

    private int writeBatchSize = 1000;

    // Do not create temporary table during insert.
    // The write operation can fail and leave the table in an inconsistent state, though it will be much faster.
    private boolean nonTransactionalInsert;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public BaseJdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public BaseJdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseJdbcConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseJdbcConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("case-insensitive-name-matching")
    public BaseJdbcConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public BaseJdbcConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    @Min(1)
    @Max(MAX_ALLOWED_WRITE_BATCH_SIZE)
    public int getWriteBatchSize()
    {
        return writeBatchSize;
    }

    @Config("write-batch-size")
    @ConfigDescription("Maximum number of rows to write in a single batch")
    public BaseJdbcConfig setWriteBatchSize(int writeBatchSize)
    {
        if (writeBatchSize < 1) {
            throw new PrestoException(CONFIGURATION_INVALID, format("%s must be greater than 0, given %s", writeBatchSize, writeBatchSize));
        }
        if (writeBatchSize > MAX_ALLOWED_WRITE_BATCH_SIZE) {
            throw new PrestoException(CONFIGURATION_INVALID, format("%s cannot exceed %s, given %s", writeBatchSize, MAX_ALLOWED_WRITE_BATCH_SIZE, writeBatchSize));
        }
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    public boolean isNonTransactionalInsert()
    {
        return nonTransactionalInsert;
    }

    @Config("non-transactional-insert-enabled")
    @ConfigDescription("Do not create temporary table during insert")
    public BaseJdbcConfig setNonTransactionalInsert(boolean nonTransactionalInsert)
    {
        this.nonTransactionalInsert = nonTransactionalInsert;
        return this;
    }
}
